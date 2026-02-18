-- Purview-Compatible Audit Events Schema
-- Extends existing core.test_runs infrastructure with Microsoft Purview-compatible audit trail
-- Schema version: 2.5 (canonical)

CREATE SCHEMA IF NOT EXISTS core;

-- ──────────────────────────────────────────────────────────────────
-- AUDIT EVENTS TABLE (Purview-compatible)
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS core.audit_events (
    -- Identity
    audit_id UUID PRIMARY KEY,
    correlation_id UUID NOT NULL,

    -- Temporal
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms INTEGER,

    -- Operation
    operation TEXT NOT NULL,                    -- e.g., "ritual.query", "ritual.wish"
    status TEXT NOT NULL CHECK (status IN ('Success', 'Failure', 'Pending')),
    result_signature TEXT DEFAULT '200',

    -- Actor & Context
    actor TEXT NOT NULL,                        -- User/actor performing operation
    client_app TEXT DEFAULT 'ritual-cli',       -- Client application
    caller_ip_address INET DEFAULT '127.0.0.1',

    -- Command Details
    command TEXT NOT NULL,                      -- e.g., "query", "wish next"
    args TEXT,                                  -- Arguments passed

    -- Federation & Routing
    federated_source TEXT DEFAULT 'local',      -- Source: local, kafka, notion, etc.
    query_intent TEXT,                          -- Semantic intent classification
    bypass_llm BOOLEAN DEFAULT FALSE,           -- Whether query bypassed LLM

    -- Trust & Compliance
    trust_score DECIMAL(3,2) DEFAULT 0.80,     -- Trust score (0.00-1.00)
    purview_compatible BOOLEAN DEFAULT TRUE,    -- Microsoft Purview format compatibility

    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,         -- Extensible metadata
    schema_version TEXT DEFAULT '2.0',          -- Audit schema version
    tenant_id TEXT DEFAULT 'spel-local-forge',  -- Tenant/forge identifier

    -- Indexes for fast querying
    CONSTRAINT audit_id_unique UNIQUE (audit_id)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON core.audit_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_correlation ON core.audit_events(correlation_id);
CREATE INDEX IF NOT EXISTS idx_audit_actor ON core.audit_events(actor);
CREATE INDEX IF NOT EXISTS idx_audit_operation ON core.audit_events(operation);
CREATE INDEX IF NOT EXISTS idx_audit_status ON core.audit_events(status);
CREATE INDEX IF NOT EXISTS idx_audit_federated_source ON core.audit_events(federated_source);

-- GIN index for JSONB metadata queries
CREATE INDEX IF NOT EXISTS idx_audit_metadata_gin ON core.audit_events USING GIN (metadata);

-- ──────────────────────────────────────────────────────────────────
-- FEDERATED SOURCES REGISTRY
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS core.federated_sources (
    source_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name TEXT UNIQUE NOT NULL,           -- e.g., "notion", "hubspot", "google"
    source_type TEXT NOT NULL,                  -- "mcp", "rest", "graphql", "kafka"
    auth_type TEXT NOT NULL,                    -- "oauth2", "apikey", "saml", "none"
    handler_path TEXT NOT NULL,                 -- Path to handler script
    audit_enabled BOOLEAN DEFAULT TRUE,
    purview_compatible BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb
);

-- ──────────────────────────────────────────────────────────────────
-- OAUTH TOKEN CACHE (for federated sources)
-- ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS core.oauth_tokens (
    token_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor TEXT NOT NULL,
    source_name TEXT NOT NULL REFERENCES core.federated_sources(source_name),
    access_token TEXT NOT NULL,                 -- Encrypted in production
    refresh_token TEXT,                         -- Encrypted in production
    token_type TEXT DEFAULT 'Bearer',
    expires_at TIMESTAMPTZ NOT NULL,
    scope TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_refreshed_at TIMESTAMPTZ,

    -- Composite unique constraint
    CONSTRAINT oauth_actor_source_unique UNIQUE (actor, source_name)
);

CREATE INDEX IF NOT EXISTS idx_oauth_actor ON core.oauth_tokens(actor);
CREATE INDEX IF NOT EXISTS idx_oauth_expires ON core.oauth_tokens(expires_at);

-- ──────────────────────────────────────────────────────────────────
-- VIEWS FOR PURVIEW EXPORT
-- ──────────────────────────────────────────────────────────────────

-- Purview-compatible audit log view (JSON format)
CREATE OR REPLACE VIEW core.purview_audit_log AS
SELECT
    jsonb_build_object(
        'time', timestamp,
        'operationName', operation,
        'category', 'FederatedQuery',
        'correlationId', correlation_id,
        'resultType', status,
        'resultSignature', result_signature,
        'durationMs', duration_ms,
        'callerIpAddress', caller_ip_address::text,
        'identity', jsonb_build_object(
            'claim', jsonb_build_array(
                jsonb_build_object(
                    'typ', 'http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn',
                    'val', actor || '@local'
                )
            )
        ),
        'properties', jsonb_build_object(
            'resource', command,
            'command', command,
            'args', args,
            'clientApp', client_app,
            'schemaVersion', schema_version,
            'auditId', audit_id,
            'tenantId', tenant_id,
            'federatedSource', federated_source,
            'bypassLLM', bypass_llm,
            'queryIntent', query_intent,
            'trustScore', trust_score
        ) || metadata
    ) AS purview_log
FROM core.audit_events
ORDER BY timestamp DESC;

-- Trust score summary view
CREATE OR REPLACE VIEW core.trust_summary AS
SELECT
    actor,
    COUNT(*) as total_operations,
    AVG(trust_score) as avg_trust_score,
    COUNT(*) FILTER (WHERE status = 'Success') as successful_ops,
    COUNT(*) FILTER (WHERE status = 'Failure') as failed_ops,
    COUNT(*) FILTER (WHERE bypass_llm = TRUE) as llm_bypassed,
    MAX(timestamp) as last_activity
FROM core.audit_events
GROUP BY actor
ORDER BY last_activity DESC;

-- ──────────────────────────────────────────────────────────────────
-- SAMPLE FEDERATED SOURCES (Microsoft-compatible)
-- ──────────────────────────────────────────────────────────────────
INSERT INTO core.federated_sources (source_name, source_type, auth_type, handler_path, metadata) VALUES
    ('notion', 'rest', 'oauth2', '/Users/peterswimm/.spel/bin/oauth_handler.sh', '{"mcp_compatible": true}'::jsonb),
    ('hubspot', 'rest', 'oauth2', '/Users/peterswimm/.spel/bin/oauth_handler.sh', '{"mcp_compatible": true}'::jsonb),
    ('google_calendar', 'rest', 'oauth2', '/Users/peterswimm/.spel/bin/oauth_handler.sh', '{"mcp_compatible": true}'::jsonb),
    ('local', 'native', 'none', '/Users/peterswimm/.spel/bin/ritual', '{"primary": true}'::jsonb),
    ('kafka', 'stream', 'none', '/Users/peterswimm/.spel/bin/ritual', '{"fallback_order": 1}'::jsonb),
    ('postgres', 'sql', 'none', 'psql', '{"fallback_order": 2}'::jsonb)
ON CONFLICT (source_name) DO NOTHING;

-- ──────────────────────────────────────────────────────────────────
-- HELPER FUNCTIONS
-- ──────────────────────────────────────────────────────────────────

-- Export last N audit events in Purview JSON format
CREATE OR REPLACE FUNCTION core.export_purview_logs(limit_count INTEGER DEFAULT 100)
RETURNS TABLE (purview_json JSONB) AS $$
BEGIN
    RETURN QUERY
    SELECT purview_log
    FROM core.purview_audit_log
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;

-- Check if OAuth token is valid
CREATE OR REPLACE FUNCTION core.is_token_valid(
    p_actor TEXT,
    p_source TEXT
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM core.oauth_tokens
        WHERE actor = p_actor
          AND source_name = p_source
          AND expires_at > NOW() + INTERVAL '5 minutes'
    );
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE core.audit_events IS 'Microsoft Purview-compatible audit trail for all ritual queries';
COMMENT ON TABLE core.federated_sources IS 'Registry of federated data sources with MCP compatibility';
COMMENT ON TABLE core.oauth_tokens IS 'OAuth 2.0 token cache for federated source authentication';
