# Audit Consumer Daemon

Event-driven Purview-compatible audit logging for SPELwork forge.

## Overview

Consumes `forge.audit.query.executed` Kafka events and writes to:
- **PostgreSQL** (`core.audit_events` table)
- **Purview JSON logs** (`/logs/purview/audit_YYYYMMDD.jsonl`)
- **Trust logs** (`/logs/trust/trust_YYYYMMDD.jsonl`)

Implements **graceful degradation**:
- Kafka down → Handler fallback to direct DB
- DB down → File logs only (no failure)

## Architecture

```
forge.audit.query.executed (Kafka)
    ↓
audit_consumer (this daemon)
    ├─ PostgreSQL: core.audit_events (async)
    ├─ Purview JSON: Microsoft-compatible format
    └─ Trust logs: SPELwork-native format

On failure → DLQ: forge.audit.query.executed.dlq
```

## Development Setup (Host)

```bash
# Install dependencies
cd /Users/peterswimm/.spel/core/python/audit_consumer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Set environment variables
export KAFKA_BROKERS="localhost:19092"
export DATABASE_URL="postgresql://peterswimm:forge@localhost:5432/forge"
export LOG_DIR="/Users/peterswimm/.spel/logs"

# Run daemon
python daemon.py
```

## Docker Deployment

### Build Image

```bash
docker build -t audit-consumer:latest .
```

### Run Container

```bash
docker run -d \
  --name forge-audit-consumer \
  --network forge-network \
  -e KAFKA_BROKERS=redpanda:9092 \
  -e DATABASE_URL=postgresql://peterswimm:forge@pg:5432/forge \
  -e LOG_DIR=/logs \
  -v /Users/peterswimm/.spel/logs:/logs \
  audit-consumer:latest
```

### Via Docker Compose

See `deploy/docker-compose.ritual.yml`:

```yaml
audit-consumer:
  build:
    context: ../core/python/audit_consumer
    dockerfile: Dockerfile
  environment:
    KAFKA_BROKERS: redpanda:9092
    DATABASE_URL: postgresql://peterswimm:${POSTGRES_PASSWORD}@pg:5432/forge
    LOG_DIR: /logs
  volumes:
    - ../logs:/logs
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:19092` | Comma-separated Kafka brokers |
| `KAFKA_GROUP_ID` | `audit-consumer` | Consumer group ID |
| `KAFKA_TOPICS` | `forge.audit.query.executed` | Topic to consume |
| `DATABASE_URL` | - | PostgreSQL connection string |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `forge` | Database name |
| `POSTGRES_USER` | `peterswimm` | Database user |
| `POSTGRES_PASSWORD` | `forge` | Database password |
| `LOG_DIR` | `/Users/peterswimm/.spel/logs` | Log directory |
| `LOG_LEVEL` | `INFO` | Logging level |
| `TENANT_ID` | `spel-local-forge` | Tenant identifier |

## Event Schema

Input event (Kafka `forge.audit.query.executed`):

```json
{
  "audit_id": "uuid",
  "correlation_id": "uuid",
  "timestamp": "2026-02-16T23:45:12.345Z",
  "operation": "ritual.query",
  "command": "query",
  "args": "what's next",
  "actor": "peterswimm",
  "status": "Success",
  "duration_ms": 58,
  "federated_source": "local",
  "query_intent": "wish_queue",
  "bypass_llm": true,
  "trust_score": 0.8,
  "metadata": {}
}
```

## Testing

### Test Event Emission

```bash
# Terminal 1: Watch Kafka topic
docker exec -it forge-redpanda \
    kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic forge.audit.query.executed \
    --from-beginning

# Terminal 2: Emit test event
echo '{
  "audit_id": "'$(uuidgen)'",
  "timestamp": "'$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")'",
  "operation": "ritual.query",
  "command": "query",
  "args": "test",
  "actor": "test-user",
  "status": "Success"
}' | docker exec -i forge-redpanda \
    kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic forge.audit.query.executed
```

### Verify Database Write

```bash
docker exec forge-single-db psql -U peterswimm -d forge -c \
    "SELECT audit_id, operation, status, actor, timestamp
     FROM core.audit_events
     ORDER BY timestamp DESC
     LIMIT 5;"
```

### Verify File Logs

```bash
# Purview logs
tail -f ~/.spel/logs/purview/audit_$(date +%Y%m%d).jsonl | jq .

# Trust logs
tail -f ~/.spel/logs/trust/trust_$(date +%Y%m%d).jsonl | jq .
```

## Dead Letter Queue

Failed events are sent to `forge.audit.query.executed.dlq`:

```bash
# View DLQ messages
docker exec -it forge-redpanda \
    kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic forge.audit.query.executed.dlq \
    --from-beginning
```

## Graceful Degradation

The daemon implements multi-layer fallback:

1. **Primary**: Kafka → DB + File logs
2. **DB down**: Kafka → File logs only (warning logged)
3. **Processing error**: Event sent to DLQ

No blocking failures - always continues processing.

## Monitoring

Health check endpoint (for Kubernetes/Docker healthcheck):
- Returns exit code 0 if running

Logs to stdout (structured logging):
```
2026-02-16 23:45:12 - audit_consumer - INFO - ✅ [uuid] Audit event processed (DB: True)
```

## See Also

- [Purview Handler README](../../../docs/PURVIEW_HANDLER_README.md)
- [Lore Guards Daemon](../lore_guards/daemon.py) - Pattern reference
- [Familiar Verifiers](../familiar_verifiers/event_consumer.py) - Pattern reference
