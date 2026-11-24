/** The lifecycle an Executor has while it runs inside the Runtime, beyond the Engine's `Executor` port. */
import type { AssessmentOutcome, Executor, Ticket } from '../core/index.ts'

/**
 * What the composition root and the Engine's `prepareTicket` hook call on the selected Executor. `DshExecutor` and
 * `AoExecutor` both provide it; `reconcile` is optional because an Executor whose sessions live elsewhere can read them
 * back after a restart, while a local one resumes through its effects.
 */
export interface ExecutorRuntime extends Executor {
  /** Populate a newly accepted Ticket's Execution metadata (short name, directory, assignment text, explicit workspace). */
  prepare(ticket: Ticket, outcome: Extract<AssessmentOutcome, { kind: 'accept' }>): Ticket
  /** Subscribe to host events, or publish the Executor's readiness. */
  install(): void
  /** Queue the pending Assignments and interrupted work a Runtime restart left. */
  resume(): void
  /** Read remote sessions back after a restart; absent when the Executor only uses local effects. */
  reconcile?(): Promise<void>
  /** Stop taking new work and wait, within a bounded time, for what runs. */
  shutdown(): Promise<void>
  /** Release in-memory subscriptions and timers. */
  dispose(): void
  /** How many Assignments currently hold an Executor slot, for the panel's Executor load. */
  activeCount(): number
}
