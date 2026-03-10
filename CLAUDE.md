# audit-events

> Toilville forge submodule. Global forge instructions: `~/.claude/CLAUDE.md`

## Role
Purview-compatible Kafka consumer. Consumes audit events from forge topics and persists them
to `compliance.lineage_events` with dead-letter queue support for failures.

## Key Commands
- See README for start command

## Submodule Rules
- Writes to `compliance` schema only — never write to `core` schema directly
- All failures go to the dead-letter queue — never silently drop events
- Purview-compatible event format required for all persisted records
- `compliance.lineage_events` is append-only — no UPDATE or DELETE
