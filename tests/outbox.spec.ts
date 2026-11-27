/** Outbox delivery when processing is paused or a Source kind registers late. */
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { z } from 'zod'
import type { OutboundMessage } from '../src/core/index.ts'
import { Commands } from '../src/infra/commands.ts'
import type { SourceKind } from '../src/sources/kind.ts'
import { feishu, startRuntime, type Host } from './fixtures/host.ts'

let h: Host
beforeEach(async () => { h = await startRuntime() })
afterEach(async () => { await h.stop() })
async function at(ms: number) {
  vi.useFakeTimers({ toFake: ['Date'], now: Date.now() + ms })
  try { await h.runtime.pump(); await h.runtime.sendReplies() } finally { vi.useRealTimers() }
}

it('holds every message while processing is paused, and sends it once processing resumes', async () => {
  const lark = vi.mocked(Commands.prototype.lark)
  await h.runtime.settings.pause(true, 'panel')
  lark.mockClear()
  h.runtime.outbox.send({ id: 'held', channel: feishu('1', 'x').channel!, text: '已合并。', kind: 'notice' })
  await h.runtime.sendReplies()
  expect([h.runtime.outbox.status('held'), lark.mock.calls.length]).toEqual(['pending', 0])
  await h.runtime.settings.pause(false, 'panel')
  await h.runtime.pump(); await h.runtime.sendReplies()
  expect(h.runtime.outbox.status('held')).toBe('sent')
  expect(JSON.stringify(lark.mock.calls)).toContain('已合并。')
})

it('keeps a message for a Source that is not installed waiting, and sends it once the Source\'s kind registers', async () => {
  const sent: OutboundMessage[] = []
  const probe: SourceKind<Record<string, never>> = { kind: 'probe', title: { zh: '探针', en: 'Probe' }, description: { zh: '', en: '' }, health: [],
    schema: z.object({}).strict(), fields: [],
    channel: { send: async (_host, message) => { sent.push(message); return 'sent' }, link: () => ({ url: '', label: { zh: '探针', en: 'Probe' } }) } }
  h.runtime.outbox.send({ id: 'waiting', channel: { source: 'probe', thread: 'probe:1', address: {} }, text: 'Done', kind: 'reply' })
  await h.runtime.sendReplies()
  expect([h.runtime.outbox.status('waiting'), h.runtime.health.reply?.state]).toEqual(['pending', 'ready'])
  h.runtime.sources.register(probe)
  // A message whose Source was not installed is tried again after 5 seconds.
  await at(5000)
  expect([h.runtime.outbox.status('waiting'), sent.map(message => message.id)]).toEqual(['sent', ['waiting']])
})

it('adds a fresh receipt reaction, drops a stale one, and skips a Source that cannot react', async () => {
  const lark = vi.mocked(Commands.prototype.lark)
  lark.mockClear()
  const fresh = new Date().toISOString()
  const stale = new Date(Date.now() - 10 * 60_000).toISOString()
  h.runtime.outbox.send({ id: 'receipt-fresh', channel: feishu('1', 'x').channel!, text: '', kind: 'reaction', at: fresh })
  h.runtime.outbox.send({ id: 'receipt-stale', channel: feishu('2', 'x').channel!, text: '', kind: 'reaction', at: stale })
  h.runtime.outbox.send({ id: 'receipt-github', channel: { source: 'github', thread: 'github:1', address: {} }, text: '', kind: 'reaction', at: fresh })
  await h.runtime.sendReplies()
  expect([h.runtime.outbox.status('receipt-fresh'), h.runtime.outbox.status('receipt-stale'), h.runtime.outbox.status('receipt-github')])
    .toEqual(['sent', 'dropped', undefined])
  expect(h.runtime.store.receipts.get('receipt-stale')?.dropped?.reason).toBe('stale')
  expect(JSON.stringify(lark.mock.calls)).toContain('/open-apis/im/v1/messages/om_1/reactions')
  expect(JSON.stringify(lark.mock.calls)).not.toContain('/open-apis/im/v1/messages/om_2/reactions')
})

it('holds a receipt reaction while paused and sends it after resume', async () => {
  const lark = vi.mocked(Commands.prototype.lark)
  await h.runtime.settings.pause(true, 'panel')
  lark.mockClear()
  h.runtime.outbox.send({ id: 'receipt-paused', channel: feishu('3', 'x').channel!, text: '', kind: 'reaction', at: new Date().toISOString() })
  await h.runtime.sendReplies()
  expect([h.runtime.outbox.status('receipt-paused'), lark.mock.calls.length]).toEqual(['pending', 0])
  await h.runtime.settings.pause(false, 'panel')
  await h.runtime.pump(); await h.runtime.sendReplies()
  expect(h.runtime.outbox.status('receipt-paused')).toBe('sent')
})
