/** Reliable outbound message delivery backed by persistent receipts and retried reply effects. */
import { RECEIPT_WINDOW_MS, type OutboundMessage, type Outbox as OutboxPort, type ReplyStatus } from '../core/index.ts'
import type { Commands } from '../infra/commands.ts'
import { checkedImText } from '../infra/text.ts'
import type { SourceRegistry } from '../sources/registry.ts'
import { FAILURES_BEFORE_ERROR, Later, type Effects } from './effects.ts'
import type { Store } from './store.ts'

export interface OutboxHost {
  readonly store: Pick<Store, 'atomic' | 'receipts'>
  readonly effects: Pick<Effects, 'enqueue' | 'cancel' | 'handle' | 'idle'>
  readonly sources: Pick<SourceRegistry, 'instance' | 'channelAllowed' | 'send' | 'react' | 'kind'>
  readonly commands: Commands
  paused(): boolean
}

/**
 * Persists outbound channel messages as receipts and delivers them sequentially through `Effects` and `SourceRegistry`.
 * Implements the core `Outbox` port for `Engine` and source contexts, ensuring per-conversation ordering for each ticket until an earlier message fails repeatedly.
 * Messages wait while processing is paused or their source is unavailable, and settle as dropped when the target conversation is ignored or gone.
 */
export class Outbox implements OutboxPort {
  constructor(private readonly host: OutboxHost) {
    host.effects.handle('reply', (_payload, id) => this.deliver(id), { limit: () => host.paused() ? 0 : 1, retry: [5000, 300_000] })
    host.effects.handle('receipt', (_payload, id) => this.deliver(id), { limit: () => host.paused() ? 0 : 4, retry: [1000, 30_000] })
  }
  /** Record a message receipt and enqueue its delivery effect atomically; duplicate message IDs are ignored. */
  send(message: OutboundMessage): void {
    // Validate IM reply length here; acknowledgements are already validated when decided.
    if (message.kind === 'reply' && message.channel.source === 'feishu') checkedImText(message.text)
    // A reaction is a receipt only for a Source that can add one; the others, such as GitHub in v1, get no record.
    if (message.kind === 'reaction' && !this.host.sources.kind(message.channel.source)?.channel?.react) return
    const { store, effects } = this.host
    store.atomic(() => { if (store.receipts.add(message)) effects.enqueue(message.kind === 'reaction' ? 'receipt' : 'reply', message.id, {}) })
  }
  status(id: string): ReplyStatus | undefined { return this.host.store.receipts.get(id)?.status }
  withdraw(id: string): void {
    const { store, effects } = this.host
    store.atomic(() => { if (store.receipts.withdraw(id)) effects.cancel('reply', id) })
  }
  /** Resolve once every outbound write, reply or receipt reaction, has been handed to its Channel. */
  async sent(): Promise<void> {
    await this.host.effects.idle('reply')
    await this.host.effects.idle('receipt')
  }
  private async deliver(id: string): Promise<void> {
    const { store, sources, commands } = this.host
    const receipt = store.receipts.get(id)
    if (receipt?.status !== 'pending') return
    if (!sources.instance(receipt.channel.source)?.enabled) throw new Later('The Source is off or not installed')
    if (!sources.channelAllowed(receipt.channel)) { store.receipts.settle(receipt, 'channelIgnored'); return }
    if (receipt.kind === 'reaction') {
      if (this.stale(receipt)) { store.receipts.settle(receipt, 'stale'); return }
      const result = await sources.react(receipt, commands)
      if (result === 'waiting') throw new Later('The Source is not installed')
      store.receipts.settle(receipt, result === 'gone' ? 'channelGone' : 'sent')
      return
    }
    // Hold later messages for the same ticket and conversation behind an earlier pending message until it exceeds the failure threshold.
    if (store.receipts.behind(id, FAILURES_BEFORE_ERROR)) throw new Later('An earlier message of this ticket to this conversation waits')
    // A permanently unreachable target (such as a recalled message or deleted Issue) settles as `channelGone` so its receipt prevents retries.
    const result = await sources.send(receipt, commands)
    if (result === 'waiting') throw new Later('The Source is not installed')
    store.receipts.settle(receipt, result === 'gone' ? 'channelGone' : 'sent')
  }
  /** True when a receipt reaction waited past its freshness window, such as through a long pause; it is dropped, not sent. */
  private stale(receipt: OutboundMessage): boolean {
    const at = receipt.at ? Date.parse(receipt.at) : NaN
    return Number.isFinite(at) && Date.now() - at > RECEIPT_WINDOW_MS
  }
}
