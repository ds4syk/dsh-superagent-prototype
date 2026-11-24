/**
 * Workspace registry and resolution for the Agent Orchestrator (AO) bridge.
 *
 * A workspace is one AO project on one repository: where Executor Sessions for its Tickets run. The registry is owned by
 * the deployment (a JSON file named by `aoRegistryFile`), never by a Ticket, and resolution is pure: it either names the
 * one workspace the Ticket belongs to or asks a person which one, and it never registers a repository it does not own.
 */
import { readFileSync } from 'node:fs'
import { z } from 'zod'

/** `http(s)://` endpoint of an AO server. */
const HttpUrl = z.string().trim().url().refine(value => /^https?:\/\//i.test(value), 'Use an http:// or https:// URL')
/** URL-path-safe workspace reference; it also names a workspace in logs and error messages. */
const RefSchema = z.string().trim().regex(/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/, 'Use letters, digits, dot, dash or underscore, starting with a letter or digit, at most 64 characters')
const Text = (max: number) => z.string().trim().max(max)
/** One AO workspace as the deployment stores it. */
const WorkspaceInputSchema = z.object({
  ref: RefSchema.describe('URL-safe workspace reference, for example fourcolor'),
  alias: Text(80).min(1).optional().describe('Human-facing name a person may use instead of the reference'),
  repoUrl: Text(500).min(1).describe('Repository the workspace works on: an https or ssh remote, or owner/name'),
  aoBaseUrl: HttpUrl.optional().describe('AO server for this workspace; the registry default when omitted'),
  projectId: Text(200).min(1).describe('AO project id'),
  harness: Text(200).default('').describe('Agent harness the AO session runs, for example claude-code'),
  model: Text(200).default('').describe('Model override, or empty for the harness default'),
  mode: z.enum(['chat', 'tui']).default('chat').describe('How input reaches the AO session'),
  branch: Text(200).default('').describe('Branch the AO session works on, or empty for the project default'),
}).strict()
/** The registry document: a default AO endpoint and its workspaces. */
const RegistryInputSchema = z.object({
  baseUrl: HttpUrl.optional().describe('AO server used by workspaces without their own `aoBaseUrl`'),
  pollMs: z.number().int().min(100).max(60_000).default(3000).describe('How often an active Assignment is polled'),
  requestTimeoutMs: z.number().int().min(500).max(120_000).default(15_000).describe('Time limit of one AO HTTP request'),
  workspaces: z.array(WorkspaceInputSchema).min(1, 'A registry needs at least one workspace'),
}).strict()
/** A resolved workspace: every field the AO execution request needs. */
export interface Workspace {
  ref: string
  alias?: string
  repoUrl: string
  /** AO server, resolved from the workspace or its registry's default. */
  aoBaseUrl: string
  projectId: string
  harness: string
  model: string
  mode: 'chat' | 'tui'
  branch: string
}
/** What the bridge knows about a Ticket when it resolves its workspace. */
export interface WorkspaceResolutionInput {
  /** An explicit selection recorded on the Ticket, such as a routing decision's `workspace`; authoritative when present. */
  explicit?: string
  /** A workspace pinned on the Ticket's Thread, for a Topic that belongs to one project. */
  pinned?: string
  /** The repository the Ticket's request came from, such as `acme/widgets`; matched by repository identity. */
  repoUrl?: string
}
/** Which rule resolved a workspace: the explicit field, the Thread pin, the source repository, or the only candidate. */
export type WorkspaceResolutionVia = 'explicit' | 'pin' | 'repo' | 'unique'
/** A workspace resolved to one entry, or a question for a person with every candidate to choose from. */
export type WorkspaceResolution =
  | { kind: 'resolved'; workspace: Workspace; via: WorkspaceResolutionVia }
  | { kind: 'ask'; candidates: readonly Workspace[] }
/** A loaded registry document together with the bridge's timing settings. */
export interface LoadedWorkspaceRegistry {
  registry: WorkspaceRegistry
  pollMs: number
  requestTimeoutMs: number
}
/** Bridge timing settings, defaulted when a caller builds a registry without a file. */
export interface WorkspaceBridgeOptions {
  pollMs?: number
  requestTimeoutMs?: number
}
/** Default poll interval for an active AO Assignment, in milliseconds. */
export const DEFAULT_POLL_MS = 3000
/** Default time limit of one AO HTTP request, in milliseconds. */
export const DEFAULT_REQUEST_TIMEOUT_MS = 15_000

const lower = (value: string): string => value.trim().toLowerCase()
const plainRepo = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/
/**
 * Canonical repository identity: `host/owner/name` for an https, ssh or SCP-style remote, and `github.com/owner/name` for a
 * bare `owner/name`, which this deployment reads as GitHub. A local `file:` URL has no cross-repository identity, so two
 * file URLs match only by their exact text.
 */
export function repoKey(value: string): string | undefined {
  const text = value.trim()
  if (!text) return undefined
  const scp = /^(?:ssh:\/\/)?git@([^:/]+)[:/](.+)$/i.exec(text)
  if (scp) return hostPathKey(scp[1]!, scp[2]!)
  if (plainRepo.test(text)) return `github.com/${text.toLowerCase()}`
  let url: URL
  try { url = new URL(text) } catch { return undefined }
  if (url.protocol !== 'http:' && url.protocol !== 'https:' && url.protocol !== 'ssh:') return undefined
  return hostPathKey(url.hostname, url.pathname)
}
/** `host/owner/name` from a host and a repository path, dropping a `.git` suffix and any deeper path. */
function hostPathKey(host: string, path: string): string | undefined {
  const parts = path.replace(/^\/+/, '').replace(/\.git$/i, '').replace(/\/+$/, '').split('/').filter(Boolean)
  if (!parts.length || !host) return undefined
  // Keep the full path: org/team nesting and GitLab subgroups make the first
  // two segments ambiguous, and a wrong repo key would route a Ticket to the
  // wrong workspace.
  return `${host.toLowerCase()}/${parts.join('/').toLowerCase()}`
}
/** Whether two repository namings identify the same repository. */
export function sameRepo(left: string, right: string): boolean {
  if (lower(left) === lower(right)) return true
  const a = repoKey(left), b = repoKey(right)
  return a !== undefined && a === b
}
/** The workspaces whose reference, alias or repository match `selector`; a repository selector also matches other namings of it. */
function matches(workspaces: readonly Workspace[], selector: string): Workspace[] {
  const needle = lower(selector)
  if (!needle) return []
  return workspaces.filter(workspace => [workspace.ref, workspace.alias ?? '', workspace.repoUrl].some(value => lower(value) === needle)
    || sameRepo(workspace.repoUrl, selector))
}
/** Resolve one workspace, or ask with the candidates. Pure: no repository is ever registered as a side effect. */
export function resolveWorkspace(input: WorkspaceResolutionInput, workspaces: readonly Workspace[]): WorkspaceResolution {
  const ask = (candidates: readonly Workspace[]): WorkspaceResolution => ({ kind: 'ask', candidates })
  const resolved = (workspace: Workspace, via: WorkspaceResolutionVia): WorkspaceResolution => ({ kind: 'resolved', workspace, via })
  // An explicit selection is authoritative: when it names nothing, asking beats silently running in another project.
  if (input.explicit?.trim()) {
    const found = matches(workspaces, input.explicit)
    return found.length === 1 ? resolved(found[0]!, 'explicit') : ask(found.length ? found : workspaces)
  }
  if (input.pinned?.trim()) {
    const found = matches(workspaces, input.pinned)
    if (found.length === 1) return resolved(found[0]!, 'pin')
    // An ambiguous pin is not evidence for another workspace; a pin that names nothing is stale and falls through.
    if (found.length > 1) return ask(found)
  }
  if (input.repoUrl?.trim()) {
    const found = matches(workspaces, input.repoUrl)
    if (found.length === 1) return resolved(found[0]!, 'repo')
    if (found.length > 1) return ask(found)
  }
  // The only workspace in the deployment is unambiguous: there is nothing else to run in.
  return workspaces.length === 1 ? resolved(workspaces[0]!, 'unique') : ask(workspaces)
}
/** Format a schema failure with the registry source and the field that broke, so startup names what to fix. */
function invalid(source: string, error: z.ZodError): Error {
  const issues = error.issues.map(issue => `${issue.path.join('.') || 'registry'}: ${issue.message}`).join('; ')
  return new Error(`${source}: ${issues}`)
}
/**
 * The deployed workspaces, with one entry per reference and alias. It only records what the deployment names; resolution
 * never adds a workspace.
 */
export class WorkspaceRegistry {
  private readonly workspaces: readonly Workspace[]
  /** Validates uniqueness; the entries are stored as given. */
  constructor(workspaces: readonly Workspace[]) {
    const refs = new Set<string>(), aliases = new Set<string>()
    for (const workspace of workspaces) {
      if (refs.has(workspace.ref)) throw new Error(`Duplicate workspace ref: ${workspace.ref}`)
      refs.add(workspace.ref)
      if (workspace.alias !== undefined) {
        if (aliases.has(workspace.alias)) throw new Error(`Duplicate workspace alias: ${workspace.alias}`)
        aliases.add(workspace.alias)
      }
    }
    this.workspaces = Object.freeze(workspaces.map(workspace => Object.freeze({ ...workspace })))
  }
  /** Every deployed workspace, in registry order. */
  list(): readonly Workspace[] { return this.workspaces }
  /** The workspace with `ref`, when the registry names one. */
  get(ref: string): Workspace | undefined { return this.workspaces.find(workspace => workspace.ref === ref) }
  /** Resolve a Ticket's workspace against this registry. */
  resolve(input: WorkspaceResolutionInput): WorkspaceResolution { return resolveWorkspace(input, this.workspaces) }
}
/** Parse and validate a registry document, resolving each workspace's AO endpoint from its own or the document default. */
export function loadWorkspaceRegistry(value: unknown, source = 'the AO workspace registry'): LoadedWorkspaceRegistry {
  const parsed = RegistryInputSchema.safeParse(value)
  if (!parsed.success) throw invalid(source, parsed.error)
  const document = parsed.data
  const workspaces = document.workspaces.map((workspace, index): Workspace => {
    const aoBaseUrl = workspace.aoBaseUrl ?? document.baseUrl
    if (!aoBaseUrl) throw new Error(`${source}: workspaces.${index}.aoBaseUrl: a workspace or the registry needs an http(s) AO endpoint`)
    const { aoBaseUrl: _ignored, ...rest } = workspace
    return { ...rest, aoBaseUrl }
  })
  return { registry: new WorkspaceRegistry(workspaces), pollMs: document.pollMs, requestTimeoutMs: document.requestTimeoutMs }
}
/**
 * The WorkspaceBridge: the registry plus the resolution a Ticket needs, kept separate from the Executor so it can be
 * tested and reused without AO. It reads one JSON document at startup and never changes it.
 */
export class WorkspaceBridge {
  /** The deployed workspaces. */
  readonly registry: WorkspaceRegistry
  /** How often an active AO Assignment is polled, in milliseconds. */
  readonly pollMs: number
  /** Time limit of one AO HTTP request, in milliseconds. */
  readonly requestTimeoutMs: number
  constructor(registry: WorkspaceRegistry, options: WorkspaceBridgeOptions = {}) {
    this.registry = registry
    this.pollMs = options.pollMs ?? DEFAULT_POLL_MS
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
  }
  /** Load a bridge from a registry JSON file; a missing or malformed file fails startup instead of yielding an empty registry. */
  static fromFile(path: string): WorkspaceBridge {
    let text: string
    try { text = readFileSync(path, 'utf8') } catch (error) { throw new Error(`Cannot read the AO workspace registry ${path}: ${error instanceof Error ? error.message : String(error)}`) }
    let value: unknown
    try { value = JSON.parse(text) } catch (error) { throw new Error(`The AO workspace registry ${path} is not JSON: ${error instanceof Error ? error.message : String(error)}`) }
    const loaded = loadWorkspaceRegistry(value, path)
    return new WorkspaceBridge(loaded.registry, { pollMs: loaded.pollMs, requestTimeoutMs: loaded.requestTimeoutMs })
  }
  /** Every deployed workspace, in registry order. */
  list(): readonly Workspace[] { return this.registry.list() }
  /** The workspace with `ref`, when the registry names one. */
  get(ref: string): Workspace | undefined { return this.registry.get(ref) }
  /** Resolve a Ticket's workspace. */
  resolve(input: WorkspaceResolutionInput): WorkspaceResolution { return this.registry.resolve(input) }
}
