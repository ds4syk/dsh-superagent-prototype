/** AO executor: contract mapping, session ensure, delivery dedupe, polled states, reconcile and idempotent stop. */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { Effects } from '../src/app/effects.ts'
import { Store } from '../src/app/store.ts'
import { HealthBoard } from '../src/infra/health.ts'
import { KV } from '../src/kv.ts'
import { UserError, type Assignment, type EngineForExecutor, type Ticket, type Trigger, type WorkerReport } from '../src/core/index.ts'
import { AoExecutor, assignmentPrompt, externalRef, mapAoExecution, requestFingerprint, type AoExecutorHost } from '../src/executor/ao.ts'
import { WorkspaceBridge, WorkspaceRegistry, type Workspace } from '../src/bridge/workspace.ts'
import { startFakeAo, type FakeAo, type FakeExecution } from './fixtures/ao.ts'

const AT = '2026-09-09T00:00:00.000Z'
const fourcolor: Workspace = { ref: 'fourcolor', alias: '四色问题', repoUrl: 'file:///srv/fourcolor', aoBaseUrl: 'http://127.0.0.1:1',
  projectId: 'fourcolor', harness: 'claude-code', model: '', mode: 'chat', branch: '' }
const widgets: Workspace = { ref: 'widgets', alias: 'Widgets', repoUrl: 'https://github.com/acme/widgets.git', aoBaseUrl: 'http://127.0.0.1:1',
  projectId: 'widgets-prod', harness: 'codex', model: 'gpt', mode: 'tui', branch: 'main' }

let directory: string | undefined
const cleanups: (() => Promise<void> | void)[] = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
  if (directory) rmSync(directory, { recursive: true, force: true })
  directory = undefined
})

interface Harness {
  fake: FakeAo
  store: Store
  effects: Effects
  executor: AoExecutor
  reports: { ticketId: string; report: WorkerReport }[]
  engine: {
    assignmentStarted: ReturnType<typeof vi.fn>
    inputsDelivered: ReturnType<typeof vi.fn>
    turnEnded: ReturnType<typeof vi.fn>
    deliveryFailed: ReturnType<typeof vi.fn>
    report: ReturnType<typeof vi.fn>
  }
  ticket: (overrides?: Partial<Ticket>) => Ticket
  assignment: () => Assignment
  addTrigger: (id?: string, text?: string) => Trigger
}

/** Build one AO Executor over a real Store and effect queue, with a recording Engine that mutates the Ticket as the real one does. */
async function harness(options: { workspaces?: Workspace[]; mode?: 'chat' | 'tui'; execution?: Partial<FakeExecution>; repo?: string;
  pollMs?: number } = {}): Promise<Harness> {
  directory = mkdtempSync(join(tmpdir(), 'ao-executor-'))
  const fake = await startFakeAo(options.execution)
  const workspaces = (options.workspaces ?? [fourcolor, widgets]).map(workspace => ({ ...workspace,
    aoBaseUrl: workspace.aoBaseUrl === 'http://127.0.0.1:1' ? fake.url : workspace.aoBaseUrl,
    ...(options.mode ? { mode: options.mode } : {}) }))
  const bridge = new WorkspaceBridge(new WorkspaceRegistry(workspaces), { pollMs: options.pollMs ?? 60_000, requestTimeoutMs: 2000 })
  const store = new Store(join(directory, 'orchestrator.sqlite'))
  const board = new HealthBoard()
  const effects = new Effects(store, board)
  effects.start()
  const reports: { ticketId: string; report: WorkerReport }[] = []
  const running = (ticketId: string) => {
    const ticket = store.tickets.get(ticketId)
    return ticket && ticket.assignments.find(assignment => assignment.state === 'running')
  }
  const engine = {
    assignmentStarted: vi.fn((ticketId: string, assignmentId: string) => {
      const ticket = store.tickets.get(ticketId)!
      const assignment = ticket.assignments.find(item => item.id === assignmentId)!
      assignment.state = 'running'; assignment.startedAt ??= AT
      store.tickets.save(ticket)
    }),
    inputsDelivered: vi.fn((ticketId: string, assignmentId: string, inputIds: string[]) => {
      const ticket = store.tickets.get(ticketId)!
      const assignment = running(ticketId)!
      ticket.undelivered = ticket.undelivered.filter(id => !inputIds.includes(id))
      assignment.deliveredAt = AT
      for (const input of ticket.inputs) if (input.kind === 'trigger' && inputIds.includes(input.id)) input.readAt ??= null
      store.tickets.save(ticket)
    }),
    turnEnded: vi.fn((ticketId: string, assignmentId: string) => {
      const ticket = store.tickets.get(ticketId)!
      const assignment = ticket.assignments.find(item => item.id === assignmentId)!
      assignment.idleAt = AT
      store.tickets.save(ticket)
    }),
    deliveryFailed: vi.fn((ticketId: string, _assignmentId: string, error: string) => {
      const ticket = store.tickets.get(ticketId)!
      ticket.delivery = { error, at: AT }
      store.tickets.save(ticket)
    }),
    report: vi.fn((ticketId: string, report: WorkerReport): Ticket => {
      const ticket = store.tickets.get(ticketId)!
      if (ticket.ended) throw new UserError('ticketEnded')
      const assignment = ticket.assignments.at(-1)!
      if (assignment.state === 'ended') throw new UserError('assignmentEnded')
      assignment.report = { state: report.state, summary: report.summary, at: AT }
      ticket.updatedAt = AT
      store.tickets.save(ticket)
      reports.push({ ticketId, report })
      return ticket
    }),
  }
  const health = board.claim('worker', { state: 'ready', detail: '', at: AT })
  const host: AoExecutorHost = { engine: engine as unknown as AoExecutorHost['engine'], store, effects, health, bridge,
    lifetime: new AbortController().signal, machine: { stateDir: directory }, closing: () => false, paused: () => false,
    sourceRepo: () => options.repo ?? 'acme/widgets', now: () => AT }
  const executor = new AoExecutor(host)
  executor.install()
  cleanups.push(async () => { await executor.shutdown(); executor.dispose(); await effects.drain(); store.close(); await fake.close() })

  const ticket = (overrides: Partial<Ticket> = {}): Ticket => {
    const id = overrides.id ?? 'tk_1'
    return {
      id, title: 'Prove four color', decisionId: 'd1',
      mandate: { strength: 'explicit', policyVersion: 'v1', basis: { via: 'addressed', addressed: 'mention' },
        actor: { source: 'feishu', id: 'ou_alice' }, scope: 'prove it' },
      channels: [], assignments: [{ id: 'asg_1', state: 'pending', createdAt: AT }], inputs: [], undelivered: [],
      createdAt: AT, updatedAt: AT, origin: { triggerId: 'trg_1', triggerIds: ['trg_1'] },
      execution: { name: 'prove-four-color', directory: join(directory!, 'work', 'prove-four-color'), assignment: 'Prove the four color theorem.' },
      ...overrides,
    }
  }
  const addTrigger = (id = 'trg_1', text = 'Prove the four color theorem'): Trigger => {
    const trigger: Trigger = { id, source: 'feishu', threads: [{ source: 'feishu', id: 'oc_team' }], actor: { source: 'feishu', id: 'ou_alice' },
      human: true, at: AT, text, url: 'https://example.test/m/1', meta: {} }
    store.triggers.save(trigger)
    return trigger
  }
  return { fake, store, effects, executor, reports, engine, ticket, assignment: () => ticket().assignments[0]!, addTrigger }
}

const seed = (h: Harness, overrides: Partial<Ticket> = {}): Ticket => {
  const ticket = h.ticket(overrides)
  h.store.tickets.save(ticket)
  h.addTrigger(ticket.origin.triggerId)
  return ticket
}

describe('AO execution contract', () => {
  it('builds a path-safe external reference and refuses ids that are not', () => {
    expect(externalRef('tk_1', 'asg_1')).toBe('sa:tk_1:asg_1')
    expect(() => externalRef('tk/1', 'asg_1')).toThrow('URL-path-safe')
    expect(() => externalRef('tk_1', 'asg/1')).toThrow('URL-path-safe')
  })

  it('fingerprints the immutable request in the reference form, and changes for every covered field', () => {
    const fields = { projectId: 'fourcolor', harness: 'claude-code', model: '', mode: 'tui', branch: '', prompt: 'prove four color', displayName: 'tk' }
    expect(requestFingerprint(fields)).toBe('sha256:f4a2f4c004c37b55dba227e0a9a764f55f0da4b086b917918b66cea0b5cd8e00')
    for (const [name, value] of Object.entries({ projectId: 'other', harness: 'codex', model: 'gpt', mode: 'chat', branch: 'main',
      prompt: 'other', displayName: 'tk2' })) {
      expect(requestFingerprint({ ...fields, [name]: value }), name).not.toBe(requestFingerprint(fields))
    }
  })

  it('maps AO execution states to one domain state, treating a failing check run as work rather than a person question', () => {
    const cases: [string, Partial<FakeExecution>, string][] = [
      ['active work', { status: 'working', activity: 'active' }, 'working'],
      ['an idle turn', { status: 'idle', activity: 'idle' }, 'waiting'],
      ['a person question', { status: 'needs_input', activity: 'waiting_input', needsPerson: true, waitingReason: 'question' }, 'blocked'],
      ['a ready pull request', { status: 'approved', activity: 'idle', prs: [{ url: 'p' }] }, 'ready'],
      ['a terminated session with a pull request', { status: 'terminated', activity: 'exited', isTerminated: true, prs: [{ url: 'p' }] }, 'ready'],
      ['a terminated session without a pull request', { status: 'terminated', activity: 'exited', isTerminated: true, prs: [] }, 'failed'],
      ['a terminated session with a stale person question', { status: 'terminated', activity: 'exited', isTerminated: true, needsPerson: true, waitingReason: 'question', prs: [] }, 'failed'],
      ['a terminated session with a pull request and a stale question', { status: 'terminated', activity: 'exited', isTerminated: true, needsPerson: true, waitingReason: 'question', prs: [{ url: 'p' }] }, 'ready'],
      ['a failing check run on an active session', { status: 'working', activity: 'active', needsPerson: false, prs: [{ url: 'p', ci: 'failure' }] }, 'working'],
      ['a CI failure status with no person needed', { status: 'ci_failed', activity: 'active', needsPerson: false, prs: [{ url: 'p' }] }, 'working'],
      ['an idle CI failure status', { status: 'ci_failed', activity: 'idle', needsPerson: false, prs: [{ url: 'p' }] }, 'waiting'],
      ['a person question even with a pull request', { needsPerson: true, waitingReason: 'question', prs: [{ url: 'p' }] }, 'blocked'],
    ]
    for (const [name, execution, state] of cases) expect(mapAoExecution(execution), name).toMatchObject({ state })
  })

  it('writes a prompt with the request, assignment and inferred-work limits', () => {
    const ticket = { title: 'Prove four color', id: 'tk_1', execution: { name: 'n', directory: '/d', assignment: 'Prove it.' },
      mandate: { strength: 'inferred' as const } } as unknown as Ticket
    const trigger = { id: 'trg_1', actor: { source: 'feishu', id: 'ou_alice' }, source: 'feishu', at: AT, text: 'Prove the four color theorem.',
      url: 'https://example.test/m/1' } as Trigger
    const prompt = assignmentPrompt(ticket, trigger)
    for (const text of ['# Prove four color', 'Ticket tk_1', 'Assignment: Prove it.', 'limited proactive authorization',
      'Prove the four color theorem.', 'https://example.test/m/1']) expect(prompt).toContain(text)
  })
})

describe('AoExecutor', () => {
  it('ensures one session, persists the mapping, starts the Assignment and polls it', async () => {
    const h = await harness()
    const ticket = seed(h)
    h.executor.start(ticket, ticket.assignments[0]!)
    await h.effects.idle()
    expect(h.fake.state.created).toBe(1)
    expect(h.engine.assignmentStarted).toHaveBeenCalledExactlyOnceWith(ticket.id, ticket.assignments[0]!.id)
    const record = h.executor.sessionRecord(ticket.id, ticket.assignments[0]!.id)!
    expect(record).toMatchObject({ externalRef: 'sa:tk_1:asg_1', workspaceRef: 'widgets', sessionId: 'fake-1', projectId: 'widgets-prod',
      mode: 'tui', branch: 'main' })
    expect(h.store.get(KV.bridge.aoSession('sa:tk_1:asg_1'))).toEqual(record)
    expect(h.executor.activeCount()).toBe(1)
    const request = h.fake.state.requests.find(item => item.method === 'PUT')!
    expect(request.body).toMatchObject({ projectId: 'widgets-prod', assignmentRef: 'asg_1', requestFingerprint: record.fingerprint,
      displayName: 'prove-four-color', harness: 'codex', model: 'gpt', mode: 'tui', branch: 'main' })
    expect(request.body?.prompt).toContain('Prove the four color theorem')
  })

  it('replays an ensure without creating a second session', async () => {
    const h = await harness()
    const ticket = seed(h)
    const assignment = ticket.assignments[0]!
    h.executor.start(ticket, assignment); await h.effects.idle()
    const first = h.executor.sessionRecord(ticket.id, assignment.id)!
    // A retry of the same Assignment is pending again locally; the same externalRef replays at AO.
    const again = h.ticket({ assignments: [{ ...assignment, state: 'pending' }] })
    h.store.tickets.save(again)
    h.executor.start(again, again.assignments[0]!)
    await h.effects.idle()
    expect(h.fake.state.created).toBe(1)
    expect(h.executor.sessionRecord(ticket.id, assignment.id)).toMatchObject({ sessionId: first.sessionId, fingerprint: first.fingerprint })
    expect(h.fake.state.requests.filter(item => item.method === 'PUT')).toHaveLength(2)
  })

  it('reports failed when the immutable request conflicts with an earlier fingerprint', async () => {
    const h = await harness()
    const ticket = seed(h)
    h.executor.start(ticket, ticket.assignments[0]!); await h.effects.idle()
    const changed = h.ticket({ execution: { name: 'prove-four-color', directory: '/d', assignment: 'A different assignment.' } })
    h.store.tickets.save(changed)
    h.executor.start(changed, changed.assignments[0]!)
    await h.effects.idle()
    expect(h.fake.state.created).toBe(1)
    expect(h.reports.at(-1)).toMatchObject({ ticketId: ticket.id, report: { state: 'failed',
      summary: expect.stringContaining('EXTERNAL_REF_CONFLICT') } })
    expect(h.executor.activeCount()).toBe(0)
    await vi.waitFor(() => expect(h.fake.state.stops.some(stop => stop.externalRef === externalRef(ticket.id, ticket.assignments[0]!.id))).toBe(true))
  })

  it('retries a non-terminal report whose Engine write failed instead of deduping it away', async () => {
    const h = await harness()
    const ticket = seed(h)
    const assignment = ticket.assignments[0]!
    h.executor.start(ticket, assignment); await h.effects.idle()
    const ref = externalRef(ticket.id, assignment.id)
    h.fake.state.execution = { ...h.fake.state.execution, status: 'working', activity: 'active', revision: 1 }
    h.engine.report.mockImplementationOnce(() => { throw new Error('transient Engine failure') })
    await h.executor.pollOnce(ref)
    expect(h.reports).toHaveLength(0)
    await h.executor.pollOnce(ref)
    expect(h.reports.map(item => item.report.state)).toEqual(['progress'])
  })

  it('delivers each waiting Input once with its id as the AO deduplication key, in chat and TUI mode', async () => {
    const h = await harness({ mode: 'chat' })
    const ticket = seed(h)
    h.executor.start(ticket, ticket.assignments[0]!); await h.effects.idle()
    const trigger = h.addTrigger('trg_2', 'Please also check the lemma.')
    const live = h.store.tickets.get(ticket.id)!
    live.inputs = [{ id: 'input-1', kind: 'text', text: 'Try the lemma.', at: AT },
      { id: 'input-2', kind: 'trigger', triggerId: trigger.id, at: AT }]
    live.undelivered = ['input-1', 'input-2']
    h.store.tickets.save(live)
    h.executor.deliver(live, live.assignments[0]!); await h.effects.idle()
    expect(h.fake.state.messages).toEqual([
      { sessionId: 'fake-1', text: 'Try the lemma.', clientMessageId: 'input-1' },
      { sessionId: 'fake-1', text: expect.stringContaining('Please also check the lemma.'), clientMessageId: 'input-2' },
    ])
    expect(h.engine.inputsDelivered.mock.calls.map(([, , ids]) => ids)).toEqual([['input-1'], ['input-2']])
    // A retried delivery of the same Input keeps its clientMessageId, and AO keeps one message.
    const replay = h.store.tickets.get(ticket.id)!
    replay.undelivered = ['input-1']
    h.store.tickets.save(replay)
    h.executor.deliver(replay, replay.assignments[0]!); await h.effects.idle()
    expect(h.fake.state.messages).toHaveLength(2)
  })

  it('sends TUI input through the send endpoint, which has no deduplication key', async () => {
    const h = await harness({ mode: 'tui' })
    const ticket = seed(h)
    h.executor.start(ticket, ticket.assignments[0]!); await h.effects.idle()
    const live = h.store.tickets.get(ticket.id)!
    live.inputs = [{ id: 'input-1', kind: 'text', text: 'Try the lemma.', at: AT }]
    live.undelivered = ['input-1']
    h.store.tickets.save(live)
    h.executor.deliver(live, live.assignments[0]!); await h.effects.idle()
    expect(h.fake.state.sends).toEqual([{ sessionId: 'fake-1', message: 'Try the lemma.' }])
    expect(h.fake.state.messages).toEqual([])
  })

  it('reports progress, a question, a waiting turn and a result once per AO revision', async () => {
    const h = await harness()
    const ticket = seed(h)
    const assignment = ticket.assignments[0]!
    h.executor.start(ticket, assignment); await h.effects.idle()
    const ref = externalRef(ticket.id, assignment.id)

    h.fake.state.execution = { ...h.fake.state.execution, status: 'working', activity: 'active', revision: 1,
      latestAssistantUpdate: 'Working on the proof.' }
    await h.executor.pollOnce(ref); await h.executor.pollOnce(ref)
    expect(h.reports.map(item => item.report.state)).toEqual(['progress'])
    expect(h.reports[0]!.report.summary).toBe('Working on the proof.')

    h.fake.state.execution = { ...h.fake.state.execution, status: 'needs_input', activity: 'waiting_input', needsPerson: true,
      waitingReason: 'Which lemma?', revision: 2 }
    await h.executor.pollOnce(ref); await h.executor.pollOnce(ref)
    expect(h.reports.at(-1)!.report).toMatchObject({ state: 'blocked', summary: 'Which lemma?' })

    h.fake.state.execution = { ...h.fake.state.execution, status: 'idle', activity: 'idle', needsPerson: false, revision: 3 }
    await h.executor.pollOnce(ref); await h.executor.pollOnce(ref)
    expect(h.engine.turnEnded).toHaveBeenCalledExactlyOnceWith(ticket.id, assignment.id)

    h.fake.state.execution = { ...h.fake.state.execution, status: 'approved', prs: [{ url: 'https://example.test/pull/1' }], needsPerson: false, revision: 4 }
    await h.executor.pollOnce(ref); await h.executor.pollOnce(ref)
    expect(h.reports.at(-1)!.report).toMatchObject({ state: 'ready' })
    // A terminal state is reported once, even if its revision changes afterwards.
    h.fake.state.execution = { ...h.fake.state.execution, revision: 5 }
    await h.executor.pollOnce(ref)
    expect(h.reports.filter(item => item.report.state === 'ready')).toHaveLength(1)
    expect(h.executor.activeCount()).toBe(0)
  })

  it('retries a terminal report whose Engine write failed instead of remembering it as reported', async () => {
    const h = await harness()
    const ticket = seed(h)
    const assignment = ticket.assignments[0]!
    h.executor.start(ticket, assignment); await h.effects.idle()
    h.fake.state.execution = { ...h.fake.state.execution, status: 'terminated', activity: 'exited', isTerminated: true, prs: [], revision: 6 }
    h.engine.report.mockImplementationOnce(() => { throw new Error('database busy') })
    const ref = externalRef(ticket.id, assignment.id)
    await h.executor.pollOnce(ref)
    expect(h.executor.sessionRecord(ticket.id, assignment.id)?.reportedTerminal).toBeUndefined()
    // The poller stays attached for the retry, which records the terminal state once.
    expect(h.executor.activeCount()).toBe(1)
    await h.executor.pollOnce(ref)
    expect(h.reports.filter(item => item.report.state === 'failed')).toHaveLength(1)
    expect(h.executor.sessionRecord(ticket.id, assignment.id)?.reportedTerminal).toBe('failed')
    expect(h.executor.activeCount()).toBe(0)
  })

  it('reports a missing session as failed once', async () => {
    const h = await harness()
    const ticket = seed(h)
    const assignment = ticket.assignments[0]!
    h.executor.start(ticket, assignment); await h.effects.idle()
    h.fake.state.sessions.clear()
    const ref = externalRef(ticket.id, assignment.id)
    await h.executor.pollOnce(ref); await h.executor.pollOnce(ref)
    expect(h.reports.map(item => item.report.state)).toEqual(['failed'])
    expect(h.reports[0]!.report.summary).toContain('404')
    expect(h.executor.sessionRecord(ticket.id, assignment.id)?.reportedTerminal).toBe('failed')
  })

  it('asks a person which workspace to use instead of registering the repository itself', async () => {
    const h = await harness({ workspaces: [fourcolor, widgets], repo: 'acme/unowned' })
    const ticket = seed(h)
    h.executor.start(ticket, ticket.assignments[0]!)
    await h.effects.idle()
    expect(h.fake.state.requests).toEqual([])
    expect(h.reports.at(-1)!.report).toMatchObject({ state: 'blocked', summary: expect.stringContaining('fourcolor (四色问题), widgets (Widgets)') })
    expect(h.executor.activeCount()).toBe(0)
  })

  it('honours an explicit workspace before the source repository', async () => {
    const h = await harness()
    const ticket = seed(h, { execution: { name: 'prove-four-color', directory: '/d', assignment: 'Prove it.', workspaceRef: 'fourcolor' } })
    h.executor.start(ticket, ticket.assignments[0]!); await h.effects.idle()
    expect(h.fake.state.requests.find(item => item.method === 'PUT')?.body).toMatchObject({ projectId: 'fourcolor' })
  })

  it('stops a session once, idempotently, and leaves no poller behind', async () => {
    const h = await harness()
    const ticket = seed(h)
    const assignment = ticket.assignments[0]!
    h.executor.start(ticket, assignment); await h.effects.idle()
    h.executor.stop(ticket, assignment, 'ticket ended'); h.executor.stop(ticket, assignment, 'ticket ended')
    await h.executor.idle()
    expect(h.fake.state.stops).toEqual([{ externalRef: 'sa:tk_1:asg_1', reason: 'ticket ended' }])
    expect(h.executor.activeCount()).toBe(0)
    h.executor.stop(ticket, assignment, 'again'); await h.executor.idle()
    expect(h.fake.state.stops).toHaveLength(1)
    expect(h.executor.sessionRecord(ticket.id, assignment.id)?.stoppedAt).toBe(AT)
  })

  it('treats a missing session on stop as already stopped', async () => {
    const h = await harness()
    const ticket = seed(h)
    const assignment = ticket.assignments[0]!
    h.executor.start(ticket, assignment); await h.effects.idle()
    h.fake.state.sessions.clear()
    h.executor.stop(ticket, assignment, 'ended'); await h.executor.idle()
    expect(h.executor.sessionRecord(ticket.id, assignment.id)?.stoppedAt).toBe(AT)
  })

  it('reconciles after a restart: reports a terminal state once, then none, and reattaches active pollers', async () => {
    const first = await harness()
    const ticket = seed(first)
    const assignment = ticket.assignments[0]!
    first.executor.start(ticket, assignment); await first.effects.idle()
    first.fake.state.execution = { ...first.fake.state.execution, status: 'terminated', activity: 'exited', isTerminated: true, prs: [], revision: 8 }
    // Simulate a restart: the first Executor is gone without having polled.
    first.executor.dispose()
    const board = new HealthBoard()
    const restarted = new Effects(first.store, board)
    restarted.start()
    const host: AoExecutorHost = { engine: first.engine as unknown as AoExecutorHost['engine'], store: first.store, effects: restarted,
      health: board.claim('worker', { state: 'ready', detail: '', at: AT }), bridge: new WorkspaceBridge(new WorkspaceRegistry([
        { ...fourcolor, aoBaseUrl: first.fake.url }, { ...widgets, aoBaseUrl: first.fake.url }]), { pollMs: 60_000 }),
      lifetime: new AbortController().signal, machine: { stateDir: directory! }, closing: () => false, paused: () => false,
      sourceRepo: () => 'acme/widgets', now: () => AT }
    const second = new AoExecutor(host)
    await second.reconcile()
    expect(first.reports.filter(item => item.report.state === 'failed')).toHaveLength(1)
    expect(second.sessionRecord(ticket.id, assignment.id)?.reportedTerminal).toBe('failed')
    // A third Runtime sees the reported terminal state and reports nothing again.
    const thirdBoard = new HealthBoard()
    const thirdEffects = new Effects(first.store, thirdBoard)
    thirdEffects.start()
    const third = new AoExecutor({ ...host, effects: thirdEffects, health: thirdBoard.claim('worker') })
    await third.reconcile()
    expect(first.reports.filter(item => item.report.state === 'failed')).toHaveLength(1)
    second.dispose(); third.dispose(); await restarted.drain(); await thirdEffects.drain()
  })

  it('reconciles a running session whose durable record was lost, and reattaches its poller', async () => {
    const h = await harness()
    const ticket = seed(h)
    const assignment = ticket.assignments[0]!
    h.executor.start(ticket, assignment); await h.effects.idle()
    h.store.delete(KV.bridge.aoSession('sa:tk_1:asg_1'))
    h.fake.state.execution = { ...h.fake.state.execution, status: 'working', activity: 'active', revision: 9, latestAssistantUpdate: 'Recovered.' }
    h.executor.dispose()
    await h.executor.reconcile()
    expect(h.executor.sessionRecord(ticket.id, assignment.id)).toMatchObject({ sessionId: 'fake-1', lastKind: 'working' })
    expect(h.reports.at(-1)!.report).toMatchObject({ state: 'progress', summary: 'Recovered.' })
    expect(h.executor.activeCount()).toBe(1)
  })

  it('reconciles a running session with no session at all as failed', async () => {
    const h = await harness()
    const ticket = seed(h, { assignments: [{ id: 'asg_1', state: 'running', createdAt: AT, startedAt: AT }] })
    await h.executor.reconcile()
    expect(h.reports.at(-1)!).toMatchObject({ ticketId: ticket.id, report: { state: 'failed', summary: expect.stringContaining('404') } })
  })
})
