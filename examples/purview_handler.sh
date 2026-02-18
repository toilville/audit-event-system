#!/bin/bash
# Purview-Compatible Audit Handler Shim (Event-Driven)
# Wraps ritual queries and emits Kafka audit events
# Thin shim: minimal overhead, event-driven architecture, graceful degradation

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUDIT_LOG_DIR="/Users/peterswimm/.spel/logs/purview"
TRUST_LOG_DIR="/Users/peterswimm/.spel/logs/trust"
mkdir -p "$AUDIT_LOG_DIR" "$TRUST_LOG_DIR"

# Kafka configuration
KAFKA_TOPIC="${KAFKA_AUDIT_TOPIC:-forge.audit.query.executed}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:19092}"

# Audit event identifiers
AUDIT_ID=$(uuidgen)
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
CORRELATION_ID="${CORRELATION_ID:-$(uuidgen)}"

# Parse command-line args
COMMAND="${1:-}"
shift
ARGS="$*"

# ──────────────────────────────────────────────────────────────────
# BUILD AUDIT EVENT (Kafka-ready JSON)
# ──────────────────────────────────────────────────────────────────
build_audit_event() {
    local operation="$1"
    local status="$2"
    local duration_ms="$3"
    local query_intent="${4:-unknown}"
    local bypass_llm="${5:-false}"
    local federated_source="${6:-local}"

    cat <<EOF
{
  "audit_id": "$AUDIT_ID",
  "correlation_id": "$CORRELATION_ID",
  "timestamp": "$TIMESTAMP",
  "operation": "$operation",
  "command": "$COMMAND",
  "args": "$ARGS",
  "actor": "${USER:-unknown}",
  "status": "$status",
  "duration_ms": $duration_ms,
  "result_signature": "200",
  "client_app": "ritual-cli",
  "caller_ip_address": "127.0.0.1",
  "federated_source": "$federated_source",
  "query_intent": "$query_intent",
  "bypass_llm": $bypass_llm,
  "trust_score": 0.8,
  "purview_compatible": true,
  "schema_version": "2.0",
  "tenant_id": "spel-local-forge",
  "metadata": {}
}
EOF
}

# ──────────────────────────────────────────────────────────────────
# EMIT TO KAFKA (primary path)
# ──────────────────────────────────────────────────────────────────
emit_to_kafka() {
    local event="$1"

    # Use Python helper script (fast, reliable)
    if command -v python3 &>/dev/null && [ -f "$SCRIPT_DIR/emit_audit_event.py" ]; then
        echo "$event" | python3 "$SCRIPT_DIR/emit_audit_event.py" \
            --topic "$KAFKA_TOPIC" \
            --brokers "$KAFKA_BROKERS" 2>&1
        return $?
    fi

    # Fallback: kafka-console-producer via Docker
    if docker ps --format '{{.Names}}' | grep -q forge-redpanda; then
        echo "$event" | docker exec -i forge-redpanda \
            kafka-console-producer \
            --bootstrap-server localhost:9092 \
            --topic "$KAFKA_TOPIC" 2>/dev/null
        return $?
    fi

    return 1
}

# ──────────────────────────────────────────────────────────────────
# FALLBACK: Direct DB write (Kafka unavailable)
# ──────────────────────────────────────────────────────────────────
write_to_db_fallback() {
    local event="$1"

    docker exec forge-single-db psql -U peterswimm -d forge <<SQL 2>/dev/null
    INSERT INTO core.audit_events (
        audit_id, correlation_id, timestamp, duration_ms,
        operation, status, result_signature,
        actor, client_app, caller_ip_address,
        command, args,
        federated_source, query_intent, bypass_llm,
        trust_score, purview_compatible,
        metadata, schema_version, tenant_id
    )
    SELECT
        '$(echo "$event" | jq -r .audit_id)',
        '$(echo "$event" | jq -r .correlation_id)',
        '$(echo "$event" | jq -r .timestamp)',
        $(echo "$event" | jq -r .duration_ms),
        '$(echo "$event" | jq -r .operation)',
        '$(echo "$event" | jq -r .status)',
        '$(echo "$event" | jq -r .result_signature)',
        '$(echo "$event" | jq -r .actor)',
        '$(echo "$event" | jq -r .client_app)',
        '$(echo "$event" | jq -r .caller_ip_address)',
        '$(echo "$event" | jq -r .command)',
        '$(echo "$event" | jq -r .args)',
        '$(echo "$event" | jq -r .federated_source)',
        '$(echo "$event" | jq -r .query_intent)',
        $(echo "$event" | jq -r .bypass_llm),
        $(echo "$event" | jq -r .trust_score),
        $(echo "$event" | jq -r .purview_compatible),
        '$(echo "$event" | jq -r .metadata)',
        '$(echo "$event" | jq -r .schema_version)',
        '$(echo "$event" | jq -r .tenant_id)'
    ON CONFLICT (audit_id) DO NOTHING;
SQL
    return $?
}

# ──────────────────────────────────────────────────────────────────
# FALLBACK: File logs only (DB + Kafka unavailable)
# ──────────────────────────────────────────────────────────────────
write_to_file_fallback() {
    local event="$1"

    # Purview JSON log
    echo "$event" | jq . >> "$AUDIT_LOG_DIR/audit_$(date +%Y%m%d).jsonl" 2>/dev/null

    # Trust log (compact)
    cat >> "$TRUST_LOG_DIR/trust_$(date +%Y%m%d).jsonl" <<EOF
{"timestamp":"$TIMESTAMP","audit_id":"$AUDIT_ID","correlation_id":"$CORRELATION_ID","operation":"$(echo "$event" | jq -r .operation)","status":"$(echo "$event" | jq -r .status)","trust_score":0.8,"actor":"${USER:-unknown}","command":"$COMMAND","args":"$ARGS"}
EOF
}

# ──────────────────────────────────────────────────────────────────
# MAIN HANDLER: Wrap ritual query with event-driven audit logging
# ──────────────────────────────────────────────────────────────────
main() {
    local start_time=$(date +%s%3N)
    local operation="ritual.$COMMAND"
    local result_status="Success"
    local exit_code=0

    # PRE-EXECUTION: Classify intent (optional, for metadata)
    local query_intent="unknown"
    local bypass_llm="false"
    local federated_source="local"

    # Optional: Query intent classification (adds ~5ms overhead)
    # Uncomment if you want intent metadata in audit logs
    # query_intent=$(ritual query "classify intent" "$COMMAND" 2>/dev/null || echo "unknown")
    # bypass_llm=$(ritual query "check bypass" "$COMMAND" 2>/dev/null || echo "false")
    # federated_source=$(ritual query "resolve source" "$COMMAND" 2>/dev/null || echo "local")

    # EXECUTION: Pass through to ritual (transparent thin shim)
    local output
    if output=$(ritual "$COMMAND" "$@" 2>&1); then
        result_status="Success"
        exit_code=0
    else
        result_status="Failure"
        exit_code=$?
    fi

    # POST-EXECUTION: Calculate duration
    local end_time=$(date +%s%3N)
    local duration_ms=$((end_time - start_time))

    # Build audit event
    local audit_event
    audit_event=$(build_audit_event "$operation" "$result_status" "$duration_ms" "$query_intent" "$bypass_llm" "$federated_source")

    # EMIT TO KAFKA (primary path - event-driven)
    if emit_to_kafka "$audit_event"; then
        # Success: Event sent to Kafka, consumer will handle DB writes and logs
        echo "$output"
        return $exit_code
    fi

    # FALLBACK 1: Kafka down, write directly to DB
    if write_to_db_fallback "$audit_event"; then
        echo "[WARN] Kafka unavailable, wrote audit to DB directly" >&2
        echo "$output"
        return $exit_code
    fi

    # FALLBACK 2: DB + Kafka down, write to files only
    write_to_file_fallback "$audit_event"
    echo "[WARN] Kafka and DB unavailable, audit logged to files only" >&2
    echo "$output"
    return $exit_code
}

# ──────────────────────────────────────────────────────────────────
# MANIFEST-DRIVEN CONFIGURATION
# ──────────────────────────────────────────────────────────────────
# Check if audit is enabled in forge-manifest.spel
MANIFEST_PATH="/Users/peterswimm/.spel/forge-manifest.spel"
if [ -f "$MANIFEST_PATH" ]; then
    AUDIT_ENABLED=$(ritual query "manifest config" "audit.enabled" 2>/dev/null || echo "true")
    if [ "$AUDIT_ENABLED" = "false" ]; then
        # Bypass audit, pass through directly
        exec ritual "$@"
    fi
fi

# Execute with event-driven audit logging
main "$@"
