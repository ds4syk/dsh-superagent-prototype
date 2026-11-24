/**
 * Agent Orchestrator (AO) executor: one AO execution session per Ticket Assignment.
 *
 * The bridge owns no agent process. It ensures an AO session idempotently through AO's execution API, delivers each waiting
 * Input with the Input id as AO's deduplication key, polls the derived execution view, and reports one domain state per
 * transition. Assignments, Inputs and their reports stay in the local Store, so a Runtime restart can reattach to the
 * remote sessions and finish a terminal state that it had not reported yet.
 */
import { createHash } from 'node:crypto'
import { z } from 'zod'
import { UserError, latestAssignment, liveTicket, runningAssignment, threadKey } from '../core/index.ts'
import type { AssessmentOutcome, Assignment, EngineForExecutor, Executor, Ticket, TicketInput, Trigger, WorkerReport } from '../core/index.ts'
import type { Machine } from '../deployment.ts'
import type { Store } from '../app/store.ts'
import { Later, type Effects } from '../app/effects.ts'
import type { HealthEntry } from '../infra/health.ts'
import { reportRelay } from '../app/notices.ts'
import { KV } from '../kv.ts'
import type { Workspace, WorkspaceBridge, WorkspaceResolutionInput } from '../bridge/workspace.ts'
import { inputMessage, mandateInstructions, ticketExecution, transient } from './dsh.ts'

/** One AO mode: chat delivers conversation messages, TUI sends terminal input. */
export type AoMode = 'chat' | 'tui'
/** The durable bridge record for one Assignment's AO session; the key is its `externalRef`. */
export interface AoSessionRecord {
  externalRef: string
  ticketId: string
  assignmentId: string
  workspaceRef: string
  aoBaseUrl: string
  projectId: string
  harness: string
  model: string
  mode: AoMode
  branch: string
  sessionId: string
  fingerprint: string
  prompt: string
  displayName: string
  createdAt: string
  updatedAt?: string
  /** The last derived state and AO revision reported, which deduplicates polls that see no change. */
  lastRevision?: number | string | null
  lastKind?: AoBridgeState
  /** The terminal state reported to the Engine; a terminal state is reported once per Assignment. */
  reportedTerminal?: 'ready' | 'failed'
  /** The Ticket or Assignment ended, so the poller stopped without reporting. */
  closedAt?: string
  /** A stop was requested but AO has not confirmed it yet; reconcile retries it after a restart. */
  stopRequestedAt?: string
  stoppedAt?: string
}
/** The domain state derived from AO's execution view; `working` maps to the Engine's `progress` report. */
export type AoBridgeState = 'working' | 'waiting' | 'blocked' | 'ready' | 'failed'
/** The execution view AO serves for a session. Unknown fields are ignored. */
export interface AoExecutionView {
  status?: string
  activity?: string
  isTerminated?: boolean
  needsPerson?: boolean
  waitingReason?: string | null
  latestAssistantUpdate?: string | null
  latestUserPrompt?: string | null
  prs?: unknown[]
  revision?: number | string | null
  updatedAt?: string | null
}
/** The subset of an AO session the bridge reads. */
export interface AoSessionView {
  sessionId: string
  externalRef?: string
  assignmentRef?: string
  projectId?: string
  execution?: AoExecutionView
}
/** What the bridge sends to ensure a session: the immutable request plus its fingerprint. */
export interface AoEnsureRequest {
  projectId: string
  assignmentRef: string
  requestFingerprint: string
  prompt: string
  displayName: string
  harness: string
  model: string
  mode: string
  branch: string
}
/** An AO HTTP failure with the status and contract error code AO returned. */
export class AoHttpError extends Error {
  /** HTTP status, or 0 when the transport failed before a response. */
  readonly status: number
  /** AO's contract error code, such as `EXTERNAL_REF_CONFLICT`, when the body named one. */
  readonly code: string
  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = 'AoHttpError'
    this.status = status
    this.code = code
  }
}
const ExecutionSchema = z.object({
  status: z.string().default(''),
  activity: z.string().default(''),
  isTerminated: z.boolean().default(false),
  needsPerson: z.boolean().default(false),
  waitingReason: z.string().nullish(),
  latestAssistantUpdate: z.string().nullish(),
  latestUserPrompt: z.string().nullish(),
  prs: z.array(z.unknown()).default([]),
  revision: z.union([z.number(), z.string()]).nullish(),
  updatedAt: z.string().nullish(),
}).passthrough()
const SessionSchema = z.object({
  sessionId: z.string().min(1),
  externalRef: z.string().optional(),
  assignmentRef: z.string().optional(),
  projectId: z.string().optional(),
  execution: ExecutionSchema.optional(),
}).passthrough()
const EnsureResponseSchema = z.object({ created: z.boolean().optional(), session: SessionSchema }).passthrough()
const GetResponseSchema = z.object({ session: SessionSchema }).passthrough()
const StopResponseSchema = z.object({ stopped: z.boolean().default(true), session: SessionSchema.optional() }).passthrough()
/** Path-safe AO state values, in the order the contract checks them. */
const PR_READY = new Set(['pr_open', 'draft', 'approved', 'mergeable', 'merged'])
const TERMINATED = new Set(['terminated', 'exited'])
const ACTIVE = new Set(['working', 'running', 'needs_input'])
/** Prefix of AO's request fingerprints, kept from the reference simulator so a bridge restart can replay its requests. */
const FINGERPRINT_PREFIX = 'sha256:'
/** How many passing failures a launch tries before it reports the Ticket failed. */
const TRANSIENT_TRIES = 3
/** How many times a stop is attempted before reconcile takes it over after a restart. */
const STOP_TRIES = 3
const STOP_DELAY_MS = 1000

const reason = (error: unknown): string => error instanceof Error ? error.message : String(error)
const now = (): string => new Date().toISOString()
/**
 * AO's `externalRef` for an Assignment: `sa:<ticketId>:<assignmentId>`. Both ids are validated as URL-path-safe, and no
 * workspace or repository name enters the reference.
 * @throws When an id contains a character that is not safe in a URL path segment.
 */
export function externalRef(ticketId: string, assignmentId: string): string {
  const safe = /^[A-Za-z0-9._~-]+$/
  if (!safe.test(ticketId) || !safe.test(assignmentId)) throw new Error(`AO externalRef needs URL-path-safe ids, got ${JSON.stringify({ ticketId, assignmentId })}`)
  return `sa:${ticketId}:${assignmentId}`
}
/** The immutable fields an AO request fingerprint covers; `assignmentRef` is fixed by the external reference. */
export interface FingerprintInput {
  projectId: string
  harness: string
  model: string
  mode: string
  branch: string
  prompt: string
  displayName: string
}
/**
 * SHA-256 of the immutable request fields, in the reference simulator's canonical JSON form (sorted keys and Python's
 * `", "`/`": "` separators), so replaying an earlier request keeps the same fingerprint. A changed field is a conflict
 * for the same `externalRef`, which AO answers with `409 EXTERNAL_REF_CONFLICT`.
 */
export function requestFingerprint(input: FingerprintInput): string {
  const ordered = { branch: input.branch, displayName: input.displayName, harness: input.harness, mode: input.mode,
    model: input.model, projectId: input.projectId, prompt: input.prompt }
  const canonical = `{${Object.entries(ordered).map(([name, value]) => `${JSON.stringify(name)}: ${JSON.stringify(value)}`).join(', ')}}`
  return `${FINGERPRINT_PREFIX}${createHash('sha256').update(canonical).digest('hex')}`
}
/**
 * Map AO's derived execution view to the domain report state. A person is needed only when AO says so; a failing check
 * run is not a person's question. A ready pull request is `ready`, a terminated session without one is `failed`, active
 * work is `working`, and an idle session waits for a Trigger.
 */
export function mapAoExecution(execution: AoExecutionView): { state: AoBridgeState; reason: string } {
  if (execution.needsPerson) return { state: 'blocked', reason: execution.waitingReason?.trim() || 'needsPerson' }
  const status = (execution.status ?? '').trim().toLowerCase()
  const activity = (execution.activity ?? '').trim().toLowerCase()
  const prs = execution.prs ?? []
  if (PR_READY.has(status)) return { state: 'ready', reason: status }
  if (TERMINATED.has(status) || execution.isTerminated) return { state: prs.length ? 'ready' : 'failed', reason: status || 'terminated' }
  if (ACTIVE.has(status) || activity === 'active') return { state: 'working', reason: status || activity }
  return { state: 'waiting', reason: status || activity || 'idle' }
}
/** The display name AO shows for a Ticket: its short worktree name, or the tail of its id. */
export const displayNameOf = (ticket: Ticket): string => ticket.execution?.name ?? ticket.id.slice(-20)
/**
 * The prompt an AO session starts with: the Ticket title, its assignment text, the limited-authority note for inferred
 * work, and the originating request. The originating Trigger is already durable in the local Store; the prompt gives the
 * remote harness enough to start without reading it.
 */
export function assignmentPrompt(ticket: Ticket, trigger?: Trigger): string {
  const assignment = ticket.execution?.assignment?.trim()
  const instruction = mandateInstructions(ticket.mandate).trim()
  const request = trigger?.text.trim()
  return [
    `# ${ticket.title}`,
    `Ticket ${ticket.id}.`,
    ...(assignment ? [`Assignment: ${assignment}`] : []),
    ...(instruction ? [instruction] : []),
    ...(request ? [`Request from ${trigger!.actor.id} on ${trigger!.source} at ${trigger!.at}:`, request] : []),
    ...(trigger?.url ? [`Source: ${trigger.url}`] : []),
  ].filter(Boolean).join('\n\n')
}
/** Host dependencies used by `AoExecutor`, the AO counterpart of the `DshExecutor` host. */
export interface AoExecutorHost {
  /** Where the Executor reports what happened to Assignments and Inputs. */
  readonly engine: Pick<EngineForExecutor, 'assignmentStarted' | 'inputsDelivered' | 'turnEnded' | 'deliveryFailed' | 'report'>
  /** Local durable state: Tickets, their Triggers and Threads, the bridge records and the effect queue's store. */
  readonly store: Pick<Store, 'tickets' | 'threads' | 'triggers' | 'get' | 'set' | 'keys' | 'settings' | 'atomic' | 'afterCommit'>
  /** Durable effect queue through which launches and input deliveries run. */
  readonly effects: Pick<Effects, 'enqueue' | 'handle'>
  /** The `worker` health entry, which reports AO poll and transport failures. */
  readonly health: HealthEntry
  /** The deployed workspace registry and its resolution. */
  readonly bridge: WorkspaceBridge
  readonly lifetime: AbortSignal
  readonly machine: Pick<Machine, 'stateDir'>
  closing(): boolean
  /** Whether processing is paused, which holds launches and input deliveries. */
  paused(): boolean
  /** The repository the deployment watches, when the source setup resolved one; the source-repository resolution step. */
  sourceRepo(): string | undefined
  /** Relays a report to the main Session, as a local executor does; omitted in embeds without one. */
  relay?(message: { id: string; text: string }): void
  /** Transport seam for embedding and tests; defaults to the global `fetch`. */
  fetch?(input: RequestInfo | URL, init?: RequestInit): Promise<Response>
  now?(): string
}
/** The AO HTTP client: one method per endpoint the bridge uses, with contract errors preserved. */
export class AoClient {
  private readonly baseUrl: string
  private readonly fetchImpl: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>
  private readonly timeoutMs: number
  constructor(baseUrl: string, options: { fetch?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>; timeoutMs?: number } = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, '')
    this.fetchImpl = options.fetch ?? ((input, init) => fetch(input, init))
    this.timeoutMs = options.timeoutMs ?? 15_000
  }
  /** Ensure the session for `externalRef`; AO answers `created` on first creation and a replay otherwise. */
  async ensure(externalRef: string, request: AoEnsureRequest): Promise<{ created: boolean; session: AoSessionView }> {
    const response = EnsureResponseSchema.parse(await this.request('PUT', `/api/v1/execution/sessions/${encodeURIComponent(externalRef)}`, request))
    return { created: response.created ?? false, session: response.session }
  }
  /** Read the session's derived execution view. */
  async get(externalRef: string): Promise<{ session: AoSessionView }> {
    return GetResponseSchema.parse(await this.request('GET', `/api/v1/execution/sessions/${encodeURIComponent(externalRef)}`))
  }
  /** Ask AO to stop the session. */
  async stop(externalRef: string, reasonText: string): Promise<{ stopped: boolean; session?: AoSessionView }> {
    return StopResponseSchema.parse(await this.request('POST', `/api/v1/execution/sessions/${encodeURIComponent(externalRef)}/stop`, { reason: reasonText }))
  }
  /** Deliver a chat message, deduplicated by `clientMessageId`. */
  async sendChat(sessionId: string, text: string, clientMessageId: string): Promise<void> {
    await this.request('POST', `/api/v1/sessions/${encodeURIComponent(sessionId)}/conversation/messages`, { text, clientMessageId })
  }
  /** Deliver TUI input. AO's TUI endpoint takes no deduplication key. */
  async sendTui(sessionId: string, message: string): Promise<void> {
    await this.request('POST', `/api/v1/sessions/${encodeURIComponent(sessionId)}/send`, { message })
  }
  private async request(method: string, path: string, body?: unknown): Promise<unknown> {
    const controller = new AbortController()
    const timer = setTimeout(() => { controller.abort(new Error(`AO request timed out after ${this.timeoutMs} ms`)) }, this.timeoutMs)
    timer.unref?.()
    let response: Response
    try {
      response = await this.fetchImpl(`${this.baseUrl}${path}`, { method, headers: { 'content-type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body), signal: controller.signal })
    } catch (error) {
      throw new AoHttpError(0, 'TRANSPORT', `AO request ${method} ${path} failed: ${reason(error)}`)
    } finally { clearTimeout(timer) }
    const text = await response.text().catch(() => '')
    let parsed: Record<string, unknown> = {}
    if (text) { try { parsed = JSON.parse(text) as Record<string, unknown> } catch { parsed = { raw: text } } }
    if (!response.ok) {
      const code = typeof parsed.code === 'string' ? parsed.code : ''
      const detail = typeof parsed.error === 'string' ? parsed.error : typeof parsed.message === 'string' ? parsed.message : response.statusText
      throw new AoHttpError(response.status, code, `AO ${method} ${path} answered ${response.status}: ${detail}`)
    }
    return parsed
  }
}
/**
 * Runs each Ticket Assignment as one AO execution session. `start` and `deliver` only queue durable effects, as the local
 * Executor does; the effects ensure the session, deliver Inputs, and report through the Engine. A per-Assignment poller
 * maps AO's execution state to `progress`, `waiting`, `blocked`, `ready` or `failed`, once per AO revision and kind.
 */
export class AoExecutor implements Executor {
  private readonly pollers = new Map<string, ReturnType<typeof setInterval>>()
  private readonly polling = new Set<string>()
  /** Async work the bridge started, which `shutdown` and tests can await. */
  private readonly tasks = new Set<Promise<void>>()
  private readonly stopping = new Map<string, Promise<void>>()
  /** Passing launch failures tried so far, by Assignment id. */
  private readonly transient = new Map<string, number>()
  /** Registers the `launch` and `delivery` effect handlers; both are held while processing is paused. */
  constructor(private readonly host: AoExecutorHost) {
    host.effects.handle('launch', payload => this.launchQueued(payload), { limit: () => host.paused() ? 0 : host.store.settings().executor.maxWorkers,
      retry: [5000, 300_000] })
    host.effects.handle('delivery', payload => this.deliverQueued(payload.ticketId), { limit: () => host.paused() ? 0 : 4, quiet: true })
  }
  /** Populates a newly accepted Ticket's Execution metadata, including an explicit workspace selection when the accept named one. */
  prepare(ticket: Ticket, outcome: Extract<AssessmentOutcome, { kind: 'accept' }>): Ticket {
    const execution = ticketExecution(ticket, outcome, this.host.store.settings(), this.host.machine.stateDir)
    return { ...ticket, execution: { ...execution, ...(outcome.workspace ? { workspaceRef: outcome.workspace } : {}) } }
  }
  /** Queue a pending Assignment's launch; the launch effect ensures its AO session. */
  start(ticket: Ticket, assignment: Assignment): void {
    this.host.effects.enqueue('launch', assignment.id, { ticketId: ticket.id, assignmentId: assignment.id })
  }
  /** Queue delivery of the Ticket's waiting Inputs to its running Assignment. */
  deliver(ticket: Ticket, _assignment: Assignment): void {
    this.host.effects.enqueue('delivery', ticket.id, { ticketId: ticket.id })
  }
  /** Launches a queued Assignment unless its Ticket ended or a newer Assignment replaced it meanwhile. */
  private async launchQueued({ ticketId, assignmentId }: { ticketId: string; assignmentId: string }): Promise<void> {
    const ticket = this.host.store.tickets.get(ticketId)
    const assignment = ticket && latestAssignment(ticket)
    if (!ticket || !liveTicket(ticket) || assignment?.id !== assignmentId || assignment.state !== 'pending') return
    try { await this.launch(ticket, assignment); this.transient.delete(assignmentId) } catch (error) {
      if (this.host.closing() || error instanceof Later) throw error
      const attempts = (this.transient.get(assignmentId) ?? 0) + 1
      // AO's PUT is idempotent, so a passing transport failure is retried by the effect queue with backoff.
      if (transient(error) && attempts < TRANSIENT_TRIES) { this.transient.set(assignmentId, attempts); throw error }
      this.transient.delete(assignmentId)
      this.failed(ticketId, reason(error))
    }
  }
  /** Resolve the workspace, ensure one AO session, persist its mapping, and tell the Engine the Assignment started. */
  private async launch(ticket: Ticket, assignment: Assignment): Promise<void> {
    const resolution = this.workspaceFor(ticket)
    if (resolution.kind === 'ask') {
      this.askWorkspace(ticket, resolution.candidates)
      return
    }
    const workspace = resolution.workspace
    const trigger = this.host.store.triggers.get(ticket.origin.triggerId)
    const prompt = assignmentPrompt(ticket, trigger)
    const displayName = displayNameOf(ticket)
    const ref = externalRef(ticket.id, assignment.id)
    const fingerprint = requestFingerprint({ projectId: workspace.projectId, harness: workspace.harness, model: workspace.model,
      mode: workspace.mode, branch: workspace.branch, prompt, displayName })
    const request: AoEnsureRequest = { projectId: workspace.projectId, assignmentRef: assignment.id, requestFingerprint: fingerprint,
      prompt, displayName, harness: workspace.harness, model: workspace.model, mode: workspace.mode, branch: workspace.branch }
    let response: { created: boolean; session: AoSessionView }
    try { response = await this.client(workspace.aoBaseUrl).ensure(ref, request) } catch (error) {
      if (error instanceof AoHttpError && error.code === 'EXTERNAL_REF_CONFLICT') {
        // The remote session belongs to a different immutable request, so polling it could never describe this Assignment.
        this.stopPoller(ref)
        const stale = this.record(ref)
        if (stale) this.save({ ...stale, reportedTerminal: 'failed', closedAt: this.now() })
        throw new Error(`The Agent Orchestrator refused ${ref}: its immutable request changed (EXTERNAL_REF_CONFLICT). A different assignment or workspace prompt already used this reference; retry the Ticket under a new Assignment.`)
      }
      throw error
    }
    const existing = this.record(ref)
    const record: AoSessionRecord = {
      externalRef: ref, ticketId: ticket.id, assignmentId: assignment.id, workspaceRef: workspace.ref, aoBaseUrl: workspace.aoBaseUrl,
      projectId: workspace.projectId, harness: workspace.harness, model: workspace.model, mode: workspace.mode, branch: workspace.branch,
      sessionId: response.session.sessionId, fingerprint, prompt, displayName,
      createdAt: existing?.createdAt ?? this.now(), updatedAt: this.now(),
      ...(existing?.lastRevision !== undefined ? { lastRevision: existing.lastRevision } : {}),
      ...(existing?.lastKind ? { lastKind: existing.lastKind } : {}),
      ...(existing?.reportedTerminal ? { reportedTerminal: existing.reportedTerminal } : {}),
    }
    this.save(record)
    // The launch may race a person's stop; a record for an ended Assignment is kept but never reported.
    const current = this.host.store.tickets.get(ticket.id)
    if (!current || !liveTicket(current) || latestAssignment(current)?.id !== assignment.id) {
      this.save({ ...record, closedAt: this.now() })
      // The Ticket ended while its session was being created, so the remote session must not outlive it.
      this.track(this.requestStop(ref, 'the ticket ended while its AO session was being created'))
      return
    }
    this.host.health.set({ state: 'ready', detail: `AO session ${record.sessionId} for ${ref}`, at: this.now() })
    this.attachPoller(ref)
    this.host.engine.assignmentStarted(ticket.id, assignment.id)
  }
  /** Delivers queued Inputs one by one, re-reading the Ticket before each so a stop ends delivery immediately. */
  private async deliverQueued(ticketId: string): Promise<void> {
    const first = this.host.store.tickets.get(ticketId)
    const target = first && liveTicket(first) ? runningAssignment(first) : undefined
    if (!first || !target || !first.undelivered.length) return
    for (;;) {
      const ticket = this.host.store.tickets.get(ticketId)
      const assignment = ticket && liveTicket(ticket) ? runningAssignment(ticket) : undefined
      const inputId = ticket?.undelivered[0]
      if (!ticket || !assignment || inputId === undefined) return
      const record = this.record(externalRef(ticket.id, assignment.id))
      // The launch effect may not have ensured the session yet; Effects retries this delivery after 2 seconds.
      if (!record?.sessionId) throw new Later('The AO session is not ready for delivery')
      if (record.stoppedAt) return
      const input = ticket.inputs.find(item => item.id === inputId)
      const text = input ? this.inputText(input) : undefined
      if (input && text !== undefined) {
        try {
          if (record.mode === 'chat') await this.client(record.aoBaseUrl).sendChat(record.sessionId, text, input.id)
          else await this.client(record.aoBaseUrl).sendTui(record.sessionId, text)
        } catch (error) {
          if (this.host.closing() || error instanceof Later) throw error
          const current = this.host.store.tickets.get(ticketId)
          if (!current || !liveTicket(current) || runningAssignment(current)?.id !== assignment.id) return
          this.host.engine.deliveryFailed(ticketId, assignment.id, reason(error))
          throw error
        }
      }
      this.host.engine.inputsDelivered(ticketId, assignment.id, [inputId])
    }
  }
  /** The text of one Input, using the local Executor's format so both Executors deliver the same content. */
  private inputText(input: TicketInput): string | undefined {
    if (input.kind !== 'trigger') return inputMessage(input)
    const trigger = this.host.store.triggers.get(input.triggerId)
    return inputMessage(input, trigger ? { trigger_id: trigger.id, from: trigger.actor.id, thread: threadKey(trigger.threads[0]!),
      at: trigger.at, text: trigger.text, ...(trigger.url ? { url: trigger.url } : {}) } : undefined)
  }
  /**
   * Ends an Assignment's work: stops its poller immediately and asks AO to stop the session after the ending transaction
   * commits. A repeated stop for the same reference is one request, and one AO has confirmed needs none.
   */
  stop(ticket: Ticket, assignment: Assignment, reasonText: string): void {
    let ref: string
    try { ref = externalRef(ticket.id, assignment.id) } catch { return }
    this.stopPoller(ref)
    this.host.store.afterCommit(() => { this.track(this.requestStop(ref, reasonText)) })
  }
  /** Ask AO to stop a session, with bounded retries; a pending request stays durable for `reconcile` after a restart. */
  private requestStop(ref: string, reasonText: string): Promise<void> {
    const running = this.stopping.get(ref)
    if (running) return running
    const task = (async () => {
      const record = this.record(ref)
      if (!record || record.stoppedAt) return
      record.stopRequestedAt = record.stopRequestedAt ?? this.now()
      this.save(record)
      let last: unknown
      for (let attempt = 0; attempt < STOP_TRIES; attempt++) {
        if (attempt) await new Promise(resolve => setTimeout(resolve, STOP_DELAY_MS * attempt).unref?.())
        try {
          await this.client(record.aoBaseUrl).stop(ref, reasonText)
          this.save({ ...record, stopRequestedAt: record.stopRequestedAt, stoppedAt: this.now() })
          this.host.health.set({ state: 'ready', detail: `AO session ${ref} stopped`, at: this.now() })
          return
        } catch (error) {
          // A missing session is already stopped: stop is idempotent across a restart.
          if (error instanceof AoHttpError && error.status === 404) {
            this.save({ ...record, stopRequestedAt: record.stopRequestedAt, stoppedAt: this.now() })
            return
          }
          last = error
        }
      }
      this.host.health.set({ state: 'error', detail: `AO stop of ${ref} failed: ${reason(last)}`, at: this.now() })
    })().finally(() => { this.stopping.delete(ref) })
    this.stopping.set(ref, task)
    return task
  }
  /**
   * Reconciles after a host restart: retries stops that AO had not confirmed, reads every running Assignment's session by
   * its derived `externalRef`, reports a missing session once as `failed`, finishes a terminal state that was not reported,
   * and reattaches a poller to every session that is still active.
   */
  async reconcile(): Promise<void> {
    for (const key of this.host.store.keys(KV.bridge.aoSession)) {
      const record = this.host.store.get(key)
      if (record?.stopRequestedAt && !record.stoppedAt) await this.requestStop(record.externalRef, 'host restarted with a pending stop')
    }
    const tickets = this.host.store.tickets.open().sort((a, b) => a.createdAt.localeCompare(b.createdAt))
    for (const ticket of tickets) {
      const assignment = runningAssignment(ticket)
      if (!assignment) continue
      const ref = externalRef(ticket.id, assignment.id)
      const settled = this.record(ref)
      if (settled?.reportedTerminal || settled?.closedAt || settled?.stoppedAt) { this.stopPoller(ref); continue }
      if (!settled) {
        const resolution = this.workspaceFor(ticket)
        if (resolution.kind === 'ask') { this.askWorkspace(ticket, resolution.candidates); continue }
        this.save(this.provisional(ticket, assignment, resolution.workspace))
      }
      await this.pollOnce(ref)
      const record = this.record(ref)
      if (record && !record.reportedTerminal && !record.closedAt && !record.stoppedAt) this.attachPoller(ref)
    }
  }
  /** Re-queues pending Assignments after a restart; running sessions are reconciled by `reconcile`. */
  resume(): void {
    for (const ticket of this.host.store.tickets.open().sort((a, b) => a.createdAt.localeCompare(b.createdAt))) {
      const latest = latestAssignment(ticket)
      if (!runningAssignment(ticket) && latest?.state === 'pending') this.start(ticket, latest)
    }
  }
  /** AO has no host events to subscribe to; the health entry states that the bridge is polling remotely. */
  install(): void { this.host.health.set({ state: 'ready', detail: 'AO Executor is ready', at: this.now() }) }
  /** Stops polling and waits for the bridge work already in flight. */
  async shutdown(): Promise<void> {
    for (const ref of [...this.pollers.keys()]) this.stopPoller(ref)
    await Promise.allSettled([...this.tasks])
  }
  /** Stops polling and drops in-memory deduplication; durable records stay for the next Runtime. */
  dispose(): void {
    for (const ref of [...this.pollers.keys()]) this.stopPoller(ref)
    this.polling.clear()
  }
  /** How many AO sessions the bridge polls, for the panel's Executor load. */
  activeCount(): number { return this.pollers.size }
  /** Reads the state of one session once; used by `reconcile` and by tests. */
  async pollOnce(ref: string): Promise<void> {
    if (this.polling.has(ref)) return
    const record = this.record(ref)
    if (!record || record.stoppedAt || record.reportedTerminal || record.closedAt) { this.stopPoller(ref); return }
    this.polling.add(ref)
    try { await this.poll(record) } finally { this.polling.delete(ref) }
  }
  private async poll(record: AoSessionRecord): Promise<void> {
    const ticket = this.host.store.tickets.get(record.ticketId)
    const assignment = ticket && liveTicket(ticket) ? runningAssignment(ticket) : undefined
    if (!ticket || !liveTicket(ticket) || assignment?.id !== record.assignmentId) { this.close(record); return }
    let response: { session: AoSessionView }
    try { response = await this.client(record.aoBaseUrl).get(record.externalRef) } catch (error) {
      // AO no longer has the session: report it once, then stop polling this Assignment.
      if (error instanceof AoHttpError && error.status === 404) { this.reportTerminal(record, 'failed', `The AO session ${record.externalRef} no longer exists (404).`); return }
      if (this.host.closing()) return
      this.host.health.set({ state: 'error', detail: `AO poll of ${record.externalRef} failed: ${reason(error)}`, at: this.now() })
      return
    }
    const execution = response.session.execution ?? {}
    const next: AoSessionRecord = { ...record, sessionId: response.session.sessionId || record.sessionId, updatedAt: this.now() }
    const mapped = mapAoExecution(execution)
    // A terminal state is reported once per Assignment, regardless of AO's revision.
    if (mapped.state === 'ready' || mapped.state === 'failed') { this.reportTerminal(next, mapped.state, this.summary(mapped, execution)); return }
    const revision = execution.revision ?? null
    if (next.lastKind === mapped.state && (revision === null || next.lastRevision === revision)) { this.save(next); return }
    next.lastKind = mapped.state
    next.lastRevision = revision
    this.save(next)
    this.report(next, mapped, execution)
  }
  /** Report one non-terminal state: `waiting` ends the turn, `working` is progress, `blocked` waits for a person. */
  private report(record: AoSessionRecord, mapped: { state: AoBridgeState; reason: string }, execution: AoExecutionView): void {
    if (mapped.state === 'waiting') {
      this.host.engine.turnEnded(record.ticketId, record.assignmentId)
      this.host.health.set({ state: 'ready', detail: `AO session ${record.externalRef} waits`, at: this.now() })
      return
    }
    const state: WorkerReport['state'] = mapped.state === 'working' ? 'progress' : 'blocked'
    this.recordReport(record, { state, summary: this.summary(mapped, execution) })
  }
  /** Report a terminal state once and stop its poller; a 404 and an ended session share this path. */
  private reportTerminal(record: AoSessionRecord, state: 'ready' | 'failed', summary: string): void {
    if (record.reportedTerminal) { this.stopPoller(record.externalRef); return }
    this.stopPoller(record.externalRef)
    const outcome = this.recordReport(record, { state, summary })
    // Save the terminal report only once the Engine recorded it, so a transient Engine failure is retried by this poller
    // rather than remembered as reported.
    if (outcome === 'reported') this.save({ ...record, reportedTerminal: state, closedAt: this.now() })
    else if (outcome === 'failed') this.attachPoller(record.externalRef)
  }
  /** Report through the Engine, relaying it to the main Session, and close the record when the Ticket or Assignment already ended. */
  private recordReport(record: AoSessionRecord, report: Pick<WorkerReport, 'state' | 'summary'>): 'reported' | 'closed' | 'failed' {
    try {
      this.host.store.atomic(() => {
        const ticket = this.host.engine.report(record.ticketId, report)
        const relay = reportRelay(ticket, report)
        if (relay) this.host.relay?.(relay)
      })
      return 'reported'
    } catch (error) {
      // A stop or a person's end may win the race with a poll; that ends this session's work without another report.
      if (error instanceof UserError) { this.close(record); this.stopPoller(record.externalRef); return 'closed' }
      this.host.health.set({ state: 'error', detail: `AO report of ${record.externalRef} failed: ${reason(error)}`, at: this.now() })
      return 'failed'
    }
  }
  /** Report a failed launch or a failed session once, relaying the failure to the main Session. */
  private failed(ticketId: string, summary: string): void {
    const ticket = this.host.store.tickets.get(ticketId)
    if (!ticket || !liveTicket(ticket)) return
    try {
      this.host.store.atomic(() => {
        const updated = this.host.engine.report(ticketId, { state: 'failed', summary })
        const relay = reportRelay(updated, { state: 'failed', summary })
        if (relay) this.host.relay?.(relay)
      })
    } catch (error) {
      if (!(error instanceof UserError)) {
        this.host.health.set({ state: 'error', detail: `AO failure report failed: ${reason(error)}`, at: this.now() })
      }
    }
  }
  /** Ask a person which workspace runs the Ticket, with the registry's candidates; no AO request is made. */
  private askWorkspace(ticket: Ticket, candidates: readonly Workspace[]): void {
    const names = candidates.length ? candidates.map(workspace => `${workspace.ref}${workspace.alias ? ` (${workspace.alias})` : ''}`).join(', ') : 'none registered'
    const summary = `No AO workspace matched this Ticket (repository ${this.host.sourceRepo() ?? 'unknown'}). Register the repository under a workspace, or set the workspace when accepting the Ticket. Candidates: ${names}.`
    try {
      this.host.store.atomic(() => {
        const updated = this.host.engine.report(ticket.id, { state: 'blocked', summary, question: `${summary} Which workspace should it use?` })
        const relay = reportRelay(updated, { state: 'blocked', summary, question: summary })
        if (relay) this.host.relay?.(relay)
      })
    } catch (error) {
      this.host.health.set({ state: 'error', detail: `AO workspace question failed: ${reason(error)}`, at: this.now() })
    }
  }
  /** Resolve the Ticket's workspace from its explicit field, Thread pin, source repository, or the only candidate. */
  private workspaceFor(ticket: Ticket): ReturnType<WorkspaceBridge['resolve']> {
    const trigger = this.host.store.triggers.get(ticket.origin.triggerId)
    const thread = trigger ? this.host.store.threads.get(threadKey(trigger.threads[0]!)) : undefined
    const pinned = thread && typeof thread.meta.workspaceRef === 'string' ? thread.meta.workspaceRef : undefined
    const explicit = ticket.execution?.workspaceRef ?? this.host.store.get(KV.bridge.ticketWorkspace(ticket.id))
    const input: WorkspaceResolutionInput = { ...(explicit ? { explicit } : {}), ...(pinned ? { pinned } : {}),
      ...(this.host.sourceRepo() ? { repoUrl: this.host.sourceRepo() } : {}) }
    return this.host.bridge.resolve(input)
  }
  /** A record for a session that this process did not create (or whose record was lost), used to read it by externalRef. */
  private provisional(ticket: Ticket, assignment: Assignment, workspace: Workspace): AoSessionRecord {
    return { externalRef: externalRef(ticket.id, assignment.id), ticketId: ticket.id, assignmentId: assignment.id, workspaceRef: workspace.ref,
      aoBaseUrl: workspace.aoBaseUrl, projectId: workspace.projectId, harness: workspace.harness, model: workspace.model,
      mode: workspace.mode, branch: workspace.branch, sessionId: '', fingerprint: '', prompt: '', displayName: displayNameOf(ticket),
      createdAt: this.now() }
  }
  /** The summary of one state: AO's latest assistant update, else its waiting reason. */
  private summary(mapped: { state: AoBridgeState; reason: string }, execution: AoExecutionView): string {
    // A person's question is the reason a blocked session stopped; the last assistant update may predate it.
    if (mapped.state === 'blocked') return execution.waitingReason?.trim() || execution.latestAssistantUpdate?.trim() || mapped.reason
    return execution.latestAssistantUpdate?.trim() || execution.waitingReason?.trim() || mapped.reason
  }
  /** The durable bridge record for `ref`, when one exists. */
  private record(ref: string): AoSessionRecord | undefined { return this.host.store.get(KV.bridge.aoSession(ref)) }
  private save(record: AoSessionRecord): void { this.host.store.set(KV.bridge.aoSession(record.externalRef), record) }
  private close(record: AoSessionRecord): void { this.save({ ...record, closedAt: record.closedAt ?? this.now() }) }
  private client(baseUrl: string): AoClient {
    return new AoClient(baseUrl, { timeoutMs: this.host.bridge.requestTimeoutMs, ...(this.host.fetch ? { fetch: this.host.fetch } : {}) })
  }
  private attachPoller(ref: string): void {
    if (this.pollers.has(ref)) return
    const timer = setInterval(() => { this.track(this.pollOnce(ref)) }, this.host.bridge.pollMs)
    timer.unref?.()
    this.pollers.set(ref, timer)
  }
  private stopPoller(ref: string): void {
    const timer = this.pollers.get(ref)
    if (timer !== undefined) { clearInterval(timer); this.pollers.delete(ref) }
  }
  private track(work: Promise<void>): void {
    const task = work.catch(error => {
      if (!this.host.closing()) this.host.health.set({ state: 'error', detail: `AO bridge task failed: ${reason(error)}`, at: this.now() })
    }).finally(() => { this.tasks.delete(task) })
    this.tasks.add(task)
  }
  /** Wait for bridge work started so far; embedders and tests use it instead of sleeping. */
  async idle(): Promise<void> { while (this.tasks.size) await Promise.allSettled([...this.tasks]) }
  /** The current bridge record for an Assignment, for diagnostics and tests. */
  sessionRecord(ticketId: string, assignmentId: string): AoSessionRecord | undefined { return this.record(externalRef(ticketId, assignmentId)) }
  /** The current time the bridge records. */
  private now(): string { return this.host.now?.() ?? now() }
}
