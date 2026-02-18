#!/usr/bin/env python3
"""
Kafka Audit Event Emitter

Reads audit event JSON from stdin and emits to Kafka topic.
Used by purview_handler.sh for event-driven audit logging.

Usage:
    echo '{"audit_id": "...", ...}' | python emit_audit_event.py
    cat event.json | python emit_audit_event.py --topic forge.audit.custom

Environment:
    KAFKA_BROKERS - Comma-separated Kafka brokers (default: localhost:19092)
"""

import sys
import json
import os
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError


def emit_event(topic: str, event: dict, brokers: list) -> bool:
    """
    Emit audit event to Kafka

    Args:
        topic: Kafka topic name
        event: Audit event dictionary
        brokers: List of Kafka broker addresses

    Returns:
        True if successful, False otherwise
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            retries=3,   # Retry failed sends
            max_block_ms=5000,  # Timeout after 5s
        )

        # Send event
        future = producer.send(topic, value=event)
        record_metadata = future.get(timeout=5)

        # Flush to ensure delivery
        producer.flush(timeout=5)
        producer.close()

        # Success
        print(f"✅ Event sent to {topic} (partition {record_metadata.partition}, offset {record_metadata.offset})",
              file=sys.stderr)
        return True

    except KafkaError as e:
        print(f"❌ Kafka error: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description='Emit audit event to Kafka')
    parser.add_argument('--topic', default='forge.audit.query.executed',
                        help='Kafka topic (default: forge.audit.query.executed)')
    parser.add_argument('--brokers', default=os.getenv('KAFKA_BROKERS', 'localhost:19092'),
                        help='Kafka brokers (default: $KAFKA_BROKERS or localhost:19092)')
    parser.add_argument('--debug', action='store_true',
                        help='Print event to stderr before sending')

    args = parser.parse_args()

    # Read event from stdin
    try:
        event = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if args.debug:
        print(f"Event: {json.dumps(event, indent=2)}", file=sys.stderr)

    # Parse brokers
    brokers = args.brokers.split(',')

    # Emit event
    success = emit_event(args.topic, event, brokers)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
