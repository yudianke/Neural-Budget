# AGENTS.md

## Essential Commands (run from repo root)

```bash
yarn typecheck    # TypeScript + lage typecheck
yarn lint:fix     # oxfmt + oxlint auto-fix
yarn test         # All tests (vitest + lage, exits on pass)
yarn test:debug   # Tests without cache
yarn start        # Dev server (port 3001)
yarn start:server-dev  # Dev server + sync server (port 5006)
```

## Critical Rules

- **Commit/PR titles**: MUST prefix with `[AI]`
- **Run commands from root**: Never `cd` into workspaces
- **PR template**: Do NOT fill in (leave blank)
- **PR label**: Add `"AI generated"`
- Full rules: [.github/agents/pr-and-commit-rules.md](.github/agents/pr-and-commit-rules.md)

## Pre-Commit Order

1. `yarn typecheck`
2. `yarn lint:fix`
3. `yarn test` (or workspace-specific)
4. Commit with `[AI]` prefix (do not skip hooks)

## Architecture

| Package | Path | Purpose |
|---------|------|---------|
| loot-core | `packages/loot-core/` | Core logic, DB, platform exports |
| desktop-client | `packages/desktop-client/` | React UI (`@actual-app/web`) |
| component-library | `packages/component-library/` | Shared components, icons |
| crdt | `packages/crdt/` | Sync/CRDT implementation |
| sync-server | `packages/sync-server/` | Express sync server |
| api | `packages/api/` | Node.js API (`@actual-app/api`) |
| desktop-electron | `packages/desktop-electron/` | Electron wrapper |

## Workspace Commands

```bash
yarn workspace @actual-app/core run test           # loot-core tests
yarn workspace @actual-app/web run e2e            # E2E tests
yarn workspace @actual-app/web run playwright test <file>  # Single E2E test
yarn workspace @actual-app/web run vrt            # Visual regression
yarn workspace docs start                         # Docs dev server
```

## Important Conventions

- **TypeScript**: Use `type` over `interface`, `satisfies` over `as`
- **Imports**: `import { type MyType }` (inline), named exports preferred
- **React Compiler**: No manual `useCallback`/`useMemo` in desktop-client
- **Hooks**: Use from `src/hooks` (not react-router), `src/redux` (not react-redux)
- **i18n**: `Trans` component over `t()`, all user strings must be translated
- **Icons**: Auto-generated in `component-library/src/icons/`, don't edit manually
- **UUID**: `import { v4 as uuidv4 } from 'uuid'` (enforced by lint)

## Restricted Patterns (enforced by lint)

- `uuid` without destructuring
- Direct color imports (use theme)
- `@actual-app/web/*` imports in `loot-core`
- `.api` or `.electron` platform imports directly
- `React.FC` / `React.*` patterns (use named imports)

## Troubleshooting

- **Lage cache issues**: `rm -rf .lage`
- **Build failures**: `rm -rf packages/*/dist packages/*/lib-dist packages/*/build && yarn install`
- **Type errors**: Check `packages/loot-core/src/types/`
- **Import issues**: Check `tsconfig.json` and package.json `exports`

## Requirements

- Node.js >=22 (`.nvmrc`: `v22/*`)
- Yarn 4.9.1 (managed by `packageManager` field)
- Build tools for native modules (`better-sqlite3`, `bcrypt`)

## Related Docs

- [PR/Commit Rules](.github/agents/pr-and-commit-rules.md)
- [Code Review Guidelines](CODE_REVIEW_GUIDELINES.md)
- [Contributing](https://actualbudget.org/docs/contributing/)
