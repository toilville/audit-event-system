"""
Configuration module for Audit Consumer Daemon

Loads config from environment variables with sensible defaults.
"""

import os
from pathlib import Path
from typing import List


class Config:
    """Audit consumer configuration"""

    # Kafka
    KAFKA_BROKERS: List[str] = os.getenv('KAFKA_BROKERS', 'localhost:19092').split(',')
    KAFKA_GROUP_ID: str = os.getenv('KAFKA_GROUP_ID', 'audit-consumer')
    KAFKA_TOPIC: str = os.getenv('KAFKA_TOPICS', 'forge.audit.query.executed')
    KAFKA_AUTO_OFFSET_RESET: str = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    KAFKA_MAX_POLL_RECORDS: int = int(os.getenv('KAFKA_MAX_POLL_RECORDS', '100'))

    # PostgreSQL
    DATABASE_URL: str = os.getenv('DATABASE_URL', '')
    POSTGRES_HOST: str = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT: int = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_DB: str = os.getenv('POSTGRES_DB', 'forge')
    POSTGRES_USER: str = os.getenv('POSTGRES_USER', 'peterswimm')
    POSTGRES_PASSWORD: str = os.getenv('POSTGRES_PASSWORD', 'forge')

    # Logging
    LOG_DIR: Path = Path(os.getenv('LOG_DIR', '/Users/peterswimm/.spel/logs'))
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    # Tenant
    TENANT_ID: str = os.getenv('TENANT_ID', 'spel-local-forge')

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
