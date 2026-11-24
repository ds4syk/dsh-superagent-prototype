/** Fake Agent Orchestrator HTTP server implementing the execution contract, for the bridge's tests. */
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'

/** The derived execution view AO serves for a session. */
export interface FakeExecution {
  status: string
  activity: string
  isTerminated: boolean
  needsPerson: boolean
  waitingReason?: string | null
  latestAssistantUpdate?: string | null
  latestUserPrompt?: string | null
  prs: { url: string; [key: string]: unknown }[]
  revision: number
  updatedAt: string
}
/** A session the fake AO created, with the fingerprint whose replay it accepts. */
export interface FakeSession { sessionId: string; fingerprint: string; assignmentRef: string; projectId: string }
/** What the fake AO received and created. */
export interface FakeAoState {
  created: number
  sessions: Map<string, FakeSession>
  execution: FakeExecution
  /** Chat deliveries, deduplicated by `clientMessageId` as AO does. */
  messages: { sessionId: string; text: string; clientMessageId: string }[]
  /** TUI deliveries, in order. */
  sends: { sessionId: string; message: string }[]
  stops: { externalRef: string; reason: string }[]
  requests: { method: string; path: string; body: Record<string, unknown> | undefined }[]
  /** When set, the next request with this method answers with this status instead. */
  failNext?: { method: string; status: number }
}
/** A running fake AO server and its recorded state. */
export interface FakeAo { url: string; state: FakeAoState; close: () => Promise<void> }

const json = (res: ServerResponse, status: number, body: unknown): void => {
  const text = JSON.stringify(body)
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) })
  res.end(text)
}
async function body(req: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = []
  for await (const chunk of req) chunks.push(Buffer.from(chunk))
  const text = Buffer.concat(chunks).toString('utf8')
  return text ? JSON.parse(text) as Record<string, unknown> : {}
}
function decoded(path: string): string {
  const segment = path.split('/').at(-1) ?? ''
  return decodeURIComponent(segment)
}
/**
 * Start a fake AO server on a loopback port. It implements exactly the endpoints the bridge uses: ensure (PUT), read
 * (GET), stop (POST), and the chat and TUI delivery endpoints. Tests mutate `state.execution` to drive the derived state.
 */
export async function startFakeAo(execution: Partial<FakeExecution> = {}): Promise<FakeAo> {
  const state: FakeAoState = { created: 0, sessions: new Map(), messages: [], sends: [], stops: [], requests: [],
    execution: { status: 'working', activity: 'active', isTerminated: false, needsPerson: false, prs: [], revision: 1,
      updatedAt: new Date().toISOString(), ...execution } }
  const session = (externalRef: string): Record<string, unknown> => {
    const entry = state.sessions.get(externalRef)!
    return { sessionId: entry.sessionId, externalRef, assignmentRef: entry.assignmentRef, projectId: entry.projectId,
      execution: { ...state.execution } }
  }
  const fail = (method: string): number | undefined => {
    const failure = state.failNext
    if (!failure || failure.method !== method) return undefined
    state.failNext = undefined
    return failure.status
  }
  const server = createServer(async (req, res) => {
    try {
      const method = req.method ?? 'GET'
      const path = new URL(req.url ?? '/', 'http://127.0.0.1').pathname
      const sent = method === 'GET' ? undefined : await body(req)
      state.requests.push({ method, path, body: sent })
      const failed = fail(method)
      if (failed !== undefined) return json(res, failed, { error: 'injected failure' })
      const executionMatch = /^\/api\/v1\/execution\/sessions\/([^/]+)$/.exec(path)
      const stopMatch = /^\/api\/v1\/execution\/sessions\/([^/]+)\/stop$/.exec(path)
      const chatMatch = /^\/api\/v1\/sessions\/([^/]+)\/conversation\/messages$/.exec(path)
      const tuiMatch = /^\/api\/v1\/sessions\/([^/]+)\/send$/.exec(path)
      if (method === 'PUT' && executionMatch) {
        const externalRef = decodeURIComponent(executionMatch[1]!)
        const existing = state.sessions.get(externalRef)
        if (existing && existing.fingerprint !== String(sent?.requestFingerprint ?? '')) {
          return json(res, 409, { error: 'conflict', code: 'EXTERNAL_REF_CONFLICT' })
        }
        if (existing) return json(res, 200, { created: false, session: session(externalRef) })
        state.created++
        const entry: FakeSession = { sessionId: `fake-${state.created}`, fingerprint: String(sent?.requestFingerprint ?? ''),
          assignmentRef: String(sent?.assignmentRef ?? ''), projectId: String(sent?.projectId ?? '') }
        state.sessions.set(externalRef, entry)
        return json(res, 201, { created: true, session: session(externalRef) })
      }
      if (method === 'GET' && executionMatch) {
        const externalRef = decodeURIComponent(executionMatch[1]!)
        if (!state.sessions.has(externalRef)) return json(res, 404, { error: 'not found' })
        return json(res, 200, { session: session(externalRef) })
      }
      if (method === 'POST' && stopMatch) {
        const externalRef = decodeURIComponent(stopMatch[1]!)
        if (!state.sessions.has(externalRef)) return json(res, 404, { error: 'not found' })
        state.stops.push({ externalRef, reason: String(sent?.reason ?? '') })
        return json(res, 200, { stopped: true, session: session(externalRef) })
      }
      if (method === 'POST' && chatMatch) {
        const sessionId = decodeURIComponent(chatMatch[1]!)
        const clientMessageId = String(sent?.clientMessageId ?? '')
        if (!state.messages.some(message => message.clientMessageId === clientMessageId)) {
          state.messages.push({ sessionId, text: String(sent?.text ?? ''), clientMessageId })
        }
        return json(res, 202, { state: 'running' })
      }
      if (method === 'POST' && tuiMatch) {
        const sessionId = decodeURIComponent(tuiMatch[1]!)
        state.sends.push({ sessionId, message: String(sent?.message ?? '') })
        return json(res, 202, { state: 'running' })
      }
      return json(res, 404, { error: `unexpected ${method} ${path}` })
    } catch (error) {
      return json(res, 500, { error: String(error) })
    }
  })
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve) })
  const { port } = server.address() as AddressInfo
  return { url: `http://127.0.0.1:${port}`, state, close: () => new Promise<void>((resolve, reject) => {
    server.closeAllConnections()
    server.close(error => error ? reject(error) : resolve())
  }) }
}
