"""
Configuration module for Audit Consumer Daemon

Loads config from environment variables with sensible defaults.
"""

import os
from pathlib import Path
from typing import List

from audit_events import forge_config


class Config:
    """Audit consumer configuration"""

    # Kafka — broker resolved from manifest; KAFKA_BROKERS env overrides
    KAFKA_BROKERS: List[str] = forge_config.kafka_broker().split(',')
    KAFKA_GROUP_ID: str = os.getenv('KAFKA_GROUP_ID', 'audit-consumer')
    KAFKA_TOPIC: str = os.getenv('KAFKA_TOPICS', 'forge.audit.query.executed')
    KAFKA_AUTO_OFFSET_RESET: str = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    KAFKA_MAX_POLL_RECORDS: int = int(os.getenv('KAFKA_MAX_POLL_RECORDS', '100'))

    # PostgreSQL — URL resolved from manifest; DATABASE_URL env overrides
    DATABASE_URL: str = forge_config.database_url()
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_DB: str = os.getenv('POSTGRES_DB', 'toilville')
    POSTGRES_USER: str = os.getenv('POSTGRES_USER', 'peterswimm')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD', '')

    # Logging
    LOG_DIR: Path = Path(os.getenv('LOG_DIR', '/Users/peterswimm/.spel/logs'))
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    # Tenant — resolved from manifest; TENANT_ID env overrides
    TENANT_ID: str = forge_config.tenant_id()

    @classmethod
    def get_db_connection_params(cls) -> dict:
        """Get database connection parameters"""
        if cls.DATABASE_URL:
            return {'dsn': cls.DATABASE_URL}
        return {
            'host': cls.POSTGRES_HOST,
            'port': cls.POSTGRES_PORT,
            'database': cls.POSTGRES_DB,
            'user': cls.POSTGRES_USER,
            'password': cls.POSTGRES_PASSWORD,
        }

    @classmethod
    def validate(cls) -> bool:
        """Validate required config"""
        if not cls.KAFKA_BROKERS:
            raise ValueError("KAFKA_BROKERS is required")
        if not cls.KAFKA_TOPIC:
            raise ValueError("KAFKA_TOPICS is required")
        return True
