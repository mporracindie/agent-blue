# Architecture

## Core boundary

`AnalyticsAgentRuntime` is the orchestrator. It uses these interfaces:

- `LlmProvider`
- `WarehouseAdapter`
- `DbtRepositoryService`
- `ConversationStore`

The runtime does not know if it is called from Slack, Web, or CLI.

## Turn flow

1. Persist user message.
2. Gather context:
   - agent profile (“soul”)
   - recent conversation history
   - dbt model index
3. Ask LLM planner to choose action:
   - direct answer
   - run Snowflake SQL
   - inspect dbt model
4. Execute selected tool/action.
5. Ask LLM to synthesize business answer (if needed).
6. Persist assistant response.

## Multi-tenant data model (SQLite today)

- `conversations`
- `messages`
- `agent_profiles`
- `tenant_repos`

## Initialization lifecycle

`init` command:

1. Generate per-tenant deploy key.
2. Persist repo URL, dbt subpath, key path, local clone path.
3. Return public key for GitHub deploy key setup.

## Planned extension points

- `adapters/warehouse/bigQueryWarehouse.ts`
- `adapters/store/postgresConversationStore.ts` (future)
- `adapters/channel/slackChannel.ts` (future)
- `adapters/channel/httpChannel.ts` (future web UI/API)
