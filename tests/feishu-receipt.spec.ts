/** Feishu receipt reaction: the raw reaction API call and how a permanently gone target is recognised. */
import { describe, expect, it, vi } from 'vitest'
import type { OutboundMessage } from '../src/core/index.ts'
import { CliError } from '../src/infra/commands.ts'
import type { SourceContext } from '../src/sources/host.ts'
import { reactFeishu, RECEIPT_EMOJI } from '../src/sources/feishu-channel.ts'

const message = (id = 'om_1'): OutboundMessage => ({ id: 'receipt', channel: { source: 'feishu', thread: 'feishu:oc_team',
  address: { chatId: 'oc_team', messageId: id } }, text: '', kind: 'reaction' })
const host = (lark: (args: string[]) => Promise<unknown>) => ({ commands: { lark } } as unknown as Pick<SourceContext, 'commands'>)

describe('Feishu receipt reaction', () => {
  it('adds the fixed emoji through the raw reaction API', async () => {
    const lark = vi.fn(async (_args: string[]) => ({}))
    await expect(reactFeishu(host(lark), message())).resolves.toBe('sent')
    expect(lark.mock.calls[0]![0]).toEqual(['api', 'POST', '/open-apis/im/v1/messages/om_1/reactions', '--data',
      JSON.stringify({ reaction_type: { emoji_type: RECEIPT_EMOJI } })])
  })
  it('treats a withdrawn target as gone', async () => {
    const lark = async (): Promise<unknown> => { throw new CliError('message withdrawn', 230011) }
    await expect(reactFeishu(host(lark), message())).resolves.toBe('gone')
  })
  it('propagates a retryable failure', async () => {
    const lark = async (): Promise<unknown> => { throw new CliError('permission denied', 99991672) }
    await expect(reactFeishu(host(lark), message())).rejects.toThrow('permission denied')
  })
})
