/** Workspace registry and resolution: explicit, pin, source repo, unique candidate, else ask. */
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import {
  WorkspaceBridge,
  WorkspaceRegistry,
  loadWorkspaceRegistry,
  repoKey,
  resolveWorkspace,
  type Workspace,
} from '../src/bridge/workspace.ts'

let directory: string | undefined
afterEach(() => { if (directory) rmSync(directory, { recursive: true, force: true }); directory = undefined })

const workspace = (overrides: Partial<Workspace> & Pick<Workspace, 'ref'>): Workspace => ({
  repoUrl: `https://github.com/acme/${overrides.ref}.git`,
  aoBaseUrl: 'http://127.0.0.1:3001',
  projectId: overrides.ref,
  harness: 'claude-code',
  model: '',
  mode: 'tui',
  branch: '',
  ...overrides,
  ref: overrides.ref,
})

const fourcolor = workspace({ ref: 'fourcolor', alias: '四色问题', repoUrl: 'file:///srv/fourcolor', projectId: 'fourcolor' })
const widgets = workspace({ ref: 'widgets', alias: 'Widgets', repoUrl: 'https://github.com/acme/widgets.git', projectId: 'widgets-prod' })
const registry = () => new WorkspaceRegistry([fourcolor, widgets])

describe('repoKey', () => {
  it('reduces GitHub URLs, ssh remotes and owner/name to one form, and keeps other hosts apart', () => {
    for (const value of ['https://github.com/acme/widgets.git', 'https://github.com/acme/widgets', 'git@github.com:acme/widgets.git',
      'ssh://git@github.com/acme/widgets.git', 'acme/widgets']) {
      expect(repoKey(value)).toBe('github.com/acme/widgets')
    }
    expect(repoKey('https://gitlab.com/acme/widgets')).toBe('gitlab.com/acme/widgets')
    expect(repoKey('file:///srv/fourcolor')).toBeUndefined()
    expect(repoKey('')).toBeUndefined()
  })
})

describe('resolveWorkspace', () => {
  const cases: { name: string; input: Parameters<typeof resolveWorkspace>[0]; via?: string; ref?: string }[] = [
    { name: 'an explicit workspace reference', input: { explicit: 'widgets', pinned: 'fourcolor', repoUrl: 'file:///srv/fourcolor' }, via: 'explicit', ref: 'widgets' },
    { name: 'an explicit alias', input: { explicit: '四色问题' }, via: 'explicit', ref: 'fourcolor' },
    { name: 'an explicit repository URL in another form', input: { explicit: 'acme/widgets' }, via: 'explicit', ref: 'widgets' },
    { name: 'a thread pin before the source repository', input: { pinned: 'fourcolor', repoUrl: 'acme/widgets' }, via: 'pin', ref: 'fourcolor' },
    { name: 'a source repository', input: { repoUrl: 'git@github.com:acme/widgets.git' }, via: 'repo', ref: 'widgets' },
    { name: 'the only registered workspace', input: {}, via: 'unique' },
  ]
  for (const item of cases) {
    it(`resolves ${item.name}`, () => {
      const result = item.ref === undefined
        ? resolveWorkspace(item.input, [fourcolor])
        : resolveWorkspace(item.input, [fourcolor, widgets])
      expect(result).toMatchObject({ kind: 'resolved', via: item.via })
      if (result.kind === 'resolved') expect(result.workspace.ref).toBe(item.ref ?? 'fourcolor')
    })
  }

  it('asks with every candidate when no rule picks one, and never registers the repository itself', () => {
    const candidates = registry()
    const result = candidates.resolve({ repoUrl: 'acme/unowned' })
    expect(result).toEqual({ kind: 'ask', candidates: [fourcolor, widgets] })
    expect(candidates.list()).toHaveLength(2)
  })

  it('knows no workspace at all: asks with no candidate', () => {
    expect(resolveWorkspace({ explicit: 'fourcolor' }, [])).toEqual({ kind: 'ask', candidates: [] })
    expect(new WorkspaceRegistry([]).resolve({})).toEqual({ kind: 'ask', candidates: [] })
  })

  it('does not fall back when an explicit workspace names none, because that selection is authoritative', () => {
    expect(resolveWorkspace({ explicit: 'missing', pinned: 'fourcolor', repoUrl: 'acme/widgets' }, [fourcolor, widgets]))
      .toEqual({ kind: 'ask', candidates: [fourcolor, widgets] })
  })

  it('falls through a stale pin to the source repository', () => {
    expect(resolveWorkspace({ pinned: 'gone', repoUrl: 'acme/widgets' }, [fourcolor, widgets]))
      .toMatchObject({ kind: 'resolved', via: 'repo', workspace: { ref: 'widgets' } })
  })
})

describe('WorkspaceRegistry and bridge', () => {
  it('rejects duplicate references and aliases', () => {
    expect(() => new WorkspaceRegistry([fourcolor, workspace({ ref: 'fourcolor' })])).toThrow('Duplicate workspace ref: fourcolor')
    expect(() => new WorkspaceRegistry([fourcolor, workspace({ ref: 'copy', alias: '四色问题' })])).toThrow('Duplicate workspace alias: 四色问题')
  })

  it('reads a registry file with a default endpoint and per-workspace overrides', () => {
    directory = mkdtempSync(join(tmpdir(), 'ao-workspaces-'))
    const path = join(directory, 'workspaces.json')
    writeFileSync(path, JSON.stringify({
      baseUrl: 'http://127.0.0.1:3001',
      pollMs: 500,
      requestTimeoutMs: 2000,
      workspaces: [
        { ref: 'fourcolor', alias: '四色问题', repoUrl: 'file:///srv/fourcolor', projectId: 'fourcolor', harness: 'claude-code', mode: 'tui' },
        { ref: 'widgets', repoUrl: 'https://github.com/acme/widgets', aoBaseUrl: 'http://127.0.0.1:3002', projectId: 'widgets-prod' },
      ],
    }))
    const bridge = WorkspaceBridge.fromFile(path)
    expect(bridge.pollMs).toBe(500)
    expect(bridge.requestTimeoutMs).toBe(2000)
    expect(bridge.list()).toEqual([
      expect.objectContaining({ ref: 'fourcolor', aoBaseUrl: 'http://127.0.0.1:3001', projectId: 'fourcolor', model: '', branch: '' }),
      expect.objectContaining({ ref: 'widgets', aoBaseUrl: 'http://127.0.0.1:3002', projectId: 'widgets-prod', harness: '', mode: 'chat' }),
    ])
  })

  it('names the file and field when the registry is invalid or has no endpoint', () => {
    expect(() => loadWorkspaceRegistry({ workspaces: [{ ref: 'bad ref', repoUrl: 'x', projectId: 'p' }] }, 'registry.json'))
      .toThrow(/registry\.json.*ref/)
    expect(() => loadWorkspaceRegistry({ workspaces: [{ ref: 'one', repoUrl: 'x', projectId: 'p' }] }, 'registry.json'))
      .toThrow(/registry\.json.*aoBaseUrl/)
    expect(() => loadWorkspaceRegistry({ workspaces: [] }, 'registry.json')).toThrow(/registry\.json.*at least one workspace/)
    expect(() => loadWorkspaceRegistry({ workspaces: [{ ref: 'one', repoUrl: 'x', projectId: 'p', aoBaseUrl: 'ftp://x' }] }))
      .toThrow(/http/)
  })

  it('reports a missing or malformed file instead of silently using an empty registry', () => {
    const root = directory = mkdtempSync(join(tmpdir(), 'ao-workspaces-'))
    expect(() => WorkspaceBridge.fromFile(join(root, 'missing.json'))).toThrow(/missing\.json/)
    const path = join(root, 'broken.json')
    writeFileSync(path, '{')
    expect(() => WorkspaceBridge.fromFile(path)).toThrow(/broken\.json/)
  })
})
