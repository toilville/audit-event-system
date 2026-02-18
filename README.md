# Audit Event System

**Purview-compatible audit event consumer for Kafka→PostgreSQL compliance logging**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Overview

Event-driven audit logging daemon that consumes Kafka events and persists them to PostgreSQL with graceful degradation. Provides Microsoft Purview-compatible event formats for enterprise compliance.

### Features

- ✅ **Kafka Event Consumption** - Subscribe to audit topics with consumer groups
- ✅ **PostgreSQL Persistence** - Structured audit events with full JSONB metadata
- ✅ **Purview Compatibility** - Microsoft Purview JSON log format
- ✅ **Trust Logging** - SPELwork trust score tracking
- ✅ **Graceful Degradation** - Falls back to file logs if DB unavailable
- ✅ **Docker Ready** - Production Dockerfile with health checks
- ✅ **Dead Letter Queue** - Failed events to DLQ topic

## Quick Start

### Installation

```bash
# Install from PyPI
pip install toilville-audit-events

# Or install from source
git clone https://github.com/toilville/audit-event-system.git
cd audit-event-system
pip install -e .
```

### Running the Daemon

```bash
# Set environment variables
export DATABASE_URL="postgresql://user:pass@localhost:5432/forge"
export KAFKA_BROKERS="localhost:19092"
export KAFKA_TOPICS="forge.audit.query.executed"
export LOG_DIR="/var/log/audit"

# Run daemon
audit-consumer
```

### Docker Deployment

```bash
# Build image
docker build -t audit-event-system .

# Run with docker-compose
docker-compose up -d
```

## Architecture

```
Kafka Topic: forge.audit.query.executed
    ↓
AuditConsumerDaemon
    ├─ PostgreSQL: audit_events.events
    ├─ Purview logs: /logs/purview/audit_YYYYMMDD.jsonl
    ├─ Trust logs: /logs/trust/trust_YYYYMMDD.jsonl
    └─ DLQ: forge.audit.query.executed.dlq
```

## Configuration

### Environment Variables

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `DATABASE_URL` | `postgresql://localhost:5432/forge` | Yes | PostgreSQL connection string |
| `KAFKA_BROKERS` | `localhost:19092` | Yes | Kafka bootstrap servers (CSV) |
| `KAFKA_TOPICS` | `forge.audit.query.executed` | Yes | Audit event topic |
| `KAFKA_GROUP_ID` | `audit-consumer` | No | Consumer group ID |
| `LOG_DIR` | `/logs` | No | Log file directory |
| `LOG_LEVEL` | `INFO` | No | Logging level |
| `TENANT_ID` | `spel-local-forge` | No | Multi-tenant identifier |

## Event Schema

```json
{
  "audit_id": "uuid",
  "correlation_id": "uuid",
  "timestamp": "2026-02-17T00:00:00Z",
  "operation": "ritual.query",
  "command": "query",
  "args": "what's next",
  "actor": "peterswimm",
  "status": "Success",
  "duration_ms": 58,
  "result_signature": "200",
  "client_app": "ritual-cli",
  "trust_score": 0.8,
  "purview_compatible": true,
  "metadata": {}
}
```

## Database Schema

Run the migration to create required tables:

```bash
psql $DATABASE_URL -f migrations/001_initial_schema.sql
```

Creates:
- `audit_events.events` - Main audit table
- `audit_events.federated_sources` - OAuth source registry
- Views for Purview export

## Examples

See `examples/` directory for:
- `emit_event.py` - Send audit events to Kafka
- `purview_handler.sh` - Wrapper for CLI tools
- `docker-compose.example.yml` - Standalone deployment

## Development

```bash
# Install dev dependencies
pip install -e .[dev]

# Run tests
pytest

# Run with coverage
pytest --cov=audit_events --cov-report=html
```

## License

Apache 2.0 - See LICENSE file

## Part of Toilville LLC

Built by [Toilville LLC](https://toilville.com) - Ethical AI consultancy focused on governance, automation, and trust-based systems.
