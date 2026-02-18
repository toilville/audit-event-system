#!/usr/bin/env python3
"""
Audit Consumer Daemon - Event-driven Purview-compatible audit logging

Listens for forge.audit.query.executed events and:
- Writes to core.audit_events table (PostgreSQL)
- Appends to trust logs (JSONL)
- Exports Purview-compatible JSON logs
- Sends failed messages to DLQ (forge.audit.query.executed.dlq)

Follows SPELwork patterns from lore_guards and familiar_verifiers.
"""

import logging
import json
import os
import sys
import signal
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path

import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('audit_consumer')


class AuditConsumerDaemon:
    """
    Event-driven audit consumer daemon

    Consumes forge.audit.query.executed events, writes to database and logs.
    Implements graceful degradation: DB down → file logs only (no failure).
    """

    def __init__(self):
        """Initialize daemon with DB and Kafka connections"""
        self.running = True

        # Kafka configuration
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:19092').split(',')
        self.kafka_group = os.getenv('KAFKA_GROUP_ID', 'audit-consumer')
        self.kafka_topic = os.getenv('KAFKA_TOPICS', 'forge.audit.query.executed')

        # Log directories
        self.log_dir = Path(os.getenv('LOG_DIR', '/Users/peterswimm/.spel/logs'))
        self.purview_log_dir = self.log_dir / 'purview'
        self.trust_log_dir = self.log_dir / 'trust'
        self.purview_log_dir.mkdir(parents=True, exist_ok=True)
        self.trust_log_dir.mkdir(parents=True, exist_ok=True)

        # Database connection (with graceful degradation)
        self.db_conn = self._init_db_connection()
        if not self.db_conn:
            logger.warning("⚠️  Database unavailable - will log to files only")

        # Kafka consumer
        try:
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_brokers,
                group_id=self.kafka_group,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                session_timeout_ms=30000,
                max_poll_records=100,
            )
            logger.info(f"✅ Kafka consumer connected: {self.kafka_brokers}")
            logger.info(f"   Subscribed to: {self.kafka_topic}")
        except Exception as e:
            logger.error(f"❌ Kafka consumer failed: {e}")
            raise

        # Kafka producer (for DLQ)
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_brokers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
            )
            logger.info("✅ Kafka producer initialized (DLQ ready)")
        except Exception as e:
            logger.error(f"❌ Kafka producer failed: {e}")
            raise

        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        logger.info("✅ Audit Consumer Daemon initialized")

    def _init_db_connection(self) -> Optional[psycopg2.extensions.connection]:
        """
        Initialize PostgreSQL connection

        Graceful degradation: If DB unavailable, log warning and continue.
        Daemon will log to files only.
        """
        try:
            database_url = os.getenv('DATABASE_URL')
            if database_url:
                conn = psycopg2.connect(database_url)
            else:
                conn = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST', 'localhost'),
                    port=int(os.getenv('POSTGRES_PORT', '5432')),
                    database=os.getenv('POSTGRES_DB', 'forge'),
                    user=os.getenv('POSTGRES_USER', 'peterswimm'),
                    password=os.getenv('POSTGRES_PASSWORD', 'forge'),
                )
            logger.info("✅ Connected to PostgreSQL")
            return conn
        except Exception as e:
            logger.warning(f"⚠️  Database connection failed: {e}")
            return None

    def _handle_signal(self, signum, frame):
        """Handle shutdown signals (SIGTERM, SIGINT)"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _write_to_db(self, event: Dict[str, Any]) -> bool:
        """
        Write audit event to core.audit_events table

        Returns:
            True if successful, False if DB unavailable
        """
        if not self.db_conn:
            return False

        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                INSERT INTO core.audit_events (
                    audit_id, correlation_id, timestamp, duration_ms,
                    operation, status, result_signature,
                    actor, client_app, caller_ip_address,
                    command, args,
                    federated_source, query_intent, bypass_llm,
                    trust_score, purview_compatible,
                    metadata, schema_version, tenant_id
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (audit_id) DO NOTHING;
                """,
                (
                    event.get('audit_id'),
                    event.get('correlation_id'),
                    event.get('timestamp'),
                    event.get('duration_ms'),
                    event.get('operation'),
                    event.get('status'),
                    event.get('result_signature', '200'),
                    event.get('actor'),
                    event.get('client_app', 'ritual-cli'),
                    event.get('caller_ip_address', '127.0.0.1'),
                    event.get('command'),
                    event.get('args'),
                    event.get('federated_source', 'local'),
                    event.get('query_intent'),
                    event.get('bypass_llm', False),
                    event.get('trust_score', 0.8),
                    event.get('purview_compatible', True),
                    json.dumps(event.get('metadata', {})),
                    event.get('schema_version', '2.0'),
                    event.get('tenant_id', 'spel-local-forge'),
                )
            )
            self.db_conn.commit()
            return True
        except psycopg2.Error as e:
            logger.warning(f"⚠️  Database write failed: {e}")
            self.db_conn.rollback()
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error writing to DB: {e}")
            return False

    def _write_purview_log(self, event: Dict[str, Any]):
        """Write Purview-compatible JSON audit log"""
        today = datetime.utcnow().strftime('%Y%m%d')
        log_file = self.purview_log_dir / f'audit_{today}.jsonl'

        # Transform to Purview format
        purview_event = {
            "time": event.get('timestamp'),
            "operationName": event.get('operation'),
            "category": "FederatedQuery",
            "correlationId": event.get('correlation_id'),
            "resultType": event.get('status'),
            "resultSignature": event.get('result_signature', '200'),
            "durationMs": event.get('duration_ms'),
            "callerIpAddress": event.get('caller_ip_address', '127.0.0.1'),
            "identity": {
                "claim": [
                    {
                        "typ": "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn",
                        "val": f"{event.get('actor')}@local"
                    }
                ]
            },
            "properties": {
                "resource": event.get('command'),
                "command": event.get('command'),
                "args": event.get('args'),
                "clientApp": event.get('client_app', 'ritual-cli'),
                "schemaVersion": event.get('schema_version', '2.0'),
                "auditId": event.get('audit_id'),
                "tenantId": event.get('tenant_id', 'spel-local-forge'),
                "federatedSource": event.get('federated_source', 'local'),
                "bypassLLM": event.get('bypass_llm', False),
                "queryIntent": event.get('query_intent'),
                "trustScore": event.get('trust_score', 0.8),
            }
        }

        try:
            with open(log_file, 'a') as f:
                f.write(json.dumps(purview_event) + '\n')
        except Exception as e:
            logger.error(f"❌ Failed to write Purview log: {e}")

    def _write_trust_log(self, event: Dict[str, Any]):
        """Write compact SPELwork trust log"""
        today = datetime.utcnow().strftime('%Y%m%d')
        log_file = self.trust_log_dir / f'trust_{today}.jsonl'

        trust_event = {
            "timestamp": event.get('timestamp'),
            "audit_id": event.get('audit_id'),
            "correlation_id": event.get('correlation_id'),
            "operation": event.get('operation'),
            "status": event.get('status'),
            "trust_score": event.get('trust_score', 0.8),
            "actor": event.get('actor'),
            "command": event.get('command'),
            "args": event.get('args'),
        }

        try:
            with open(log_file, 'a') as f:
                f.write(json.dumps(trust_event) + '\n')
        except Exception as e:
            logger.error(f"❌ Failed to write trust log: {e}")

    def _send_to_dlq(self, event: Dict[str, Any], error: str):
        """Send failed event to Dead Letter Queue"""
        dlq_topic = f"{self.kafka_topic}.dlq"
        dlq_event = {
            "original_topic": self.kafka_topic,
            "original_event": event,
            "error": error,
            "timestamp": datetime.utcnow().isoformat() + 'Z',
        }

        try:
            self.producer.send(dlq_topic, value=dlq_event)
            self.producer.flush()
            logger.warning(f"⚠️  Event sent to DLQ: {dlq_topic}")
        except Exception as e:
            logger.error(f"❌ Failed to send to DLQ: {e}")

    def _process_event(self, event: Dict[str, Any]):
        """
        Process audit event

        Multi-layer logging with graceful degradation:
        1. Try PostgreSQL (if available)
        2. Write Purview JSON log (always)
        3. Write trust log (always)
        4. On complete failure → send to DLQ
        """
        audit_id = event.get('audit_id', 'unknown')

        try:
            # Attempt database write (graceful degradation)
            db_success = self._write_to_db(event)
            if not db_success:
                logger.debug(f"[{audit_id}] DB unavailable, file logs only")

            # Always write file logs
            self._write_purview_log(event)
            self._write_trust_log(event)

            logger.info(f"✅ [{audit_id}] Audit event processed (DB: {db_success})")

        except Exception as e:
            error_msg = f"Failed to process event: {e}"
            logger.error(f"❌ [{audit_id}] {error_msg}")
            self._send_to_dlq(event, error_msg)

    def run(self):
        """Main event loop"""
        logger.info("🚀 Audit Consumer Daemon running...")
        logger.info(f"   Kafka: {self.kafka_brokers}")
        logger.info(f"   Topic: {self.kafka_topic}")
        logger.info(f"   Logs: {self.log_dir}")
        logger.info(f"   DB: {'✅ Connected' if self.db_conn else '❌ Degraded mode (files only)'}")

        try:
            for message in self.consumer:
                if not self.running:
                    break

                try:
                    event = message.value
                    self._process_event(event)
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("⚠️  Interrupted by user")
        finally:
            self._shutdown()

    def _shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down...")

        if self.consumer:
            self.consumer.close()
            logger.info("✅ Kafka consumer closed")

        if self.producer:
            self.producer.close()
            logger.info("✅ Kafka producer closed")

        if self.db_conn:
            self.db_conn.close()
            logger.info("✅ Database connection closed")

        logger.info("✅ Audit Consumer Daemon stopped")


def main():
    """Entry point"""
    try:
        daemon = AuditConsumerDaemon()
        daemon.run()
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
