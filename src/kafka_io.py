"""
Deliverable 1: Ingestion via Kafka with Pydantic schema validation.
Real kafka-python producer/consumer with dead-letter queue routing.
"""

import json
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, Dict, Any
import traceback

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

from src.config import KAFKA_BOOTSTRAP, TOPIC_RAW, TOPIC_VALID, TOPIC_DLQ


class TicketRecord(BaseModel):
    """Pydantic schema: data contract for inbound records."""
    ticket_id: str
    customer_name: str
    priority: str
    status: str
    satisfaction_score: Optional[float] = None
    resolution_time: Optional[float] = None
    description: Optional[str] = None

    class Config:
        extra = "forbid"  # Reject unknown fields


def get_producer():
    """Return real Kafka producer or None if unavailable."""
    if not HAS_KAFKA:
        print("⚠️ Kafka not available; producer disabled")
        return None
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )
    except Exception as e:
        print(f"❌ Kafka producer failed: {e}")
        return None


def get_consumer(topic):
    """Return real Kafka consumer or None if unavailable."""
    if not HAS_KAFKA:
        print("⚠️ Kafka not available; consumer disabled")
        return None
    try:
        return KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,
        )
    except Exception as e:
        print(f"❌ Kafka consumer failed: {e}")
        return None


def validate_record(record: Dict[str, Any]) -> tuple[bool, Optional[str], Optional[TicketRecord]]:
    """
    Validate a record against TicketRecord schema.
    Returns: (is_valid, rejection_reason, validated_record)
    """
    try:
        ticket = TicketRecord(**record)
        return True, None, ticket
    except ValidationError as e:
        errors = e.errors()
        rejection_reason = " | ".join([f"{err['loc'][0]}: {err['msg']}" for err in errors])
        return False, rejection_reason, None
    except Exception as e:
        return False, str(e), None


def produce_records(producer, records: list[Dict[str, Any]]) -> Dict[str, int]:
    """
    Produce records to TOPIC_RAW. Blocks until all sent or timeout.
    Returns: {"produced": N, "failed": M}
    """
    if not producer:
        return {"produced": 0, "failed": len(records)}

    sent, failed = 0, 0
    for record in records:
        try:
            future = producer.send(TOPIC_RAW, value=record)
            future.get(timeout=10)  # Wait for confirmation
            sent += 1
        except KafkaError as e:
            print(f"❌ Failed to produce {record.get('ticket_id', '?')}: {e}")
            failed += 1

    producer.flush()
    return {"produced": sent, "failed": failed}


def consume_and_validate(topic=TOPIC_RAW, quarantine_callback=None) -> Dict[str, Any]:
    """
    Consume records from topic, validate each, route to TOPIC_VALID or TOPIC_DLQ.
    Optionally log quarantine records via callback.
    Returns: {"valid": N, "invalid": M, "dlq_routed": M}
    """
    consumer = get_consumer(topic)
    if not consumer:
        return {"valid": 0, "invalid": 0, "dlq_routed": 0, "error": "Consumer unavailable"}

    valid_producer = get_producer()
    dlq_producer = get_producer()
    valid_count, invalid_count, dlq_count = 0, 0, 0

    for message in consumer:
        record = message.value
        is_valid, rejection_reason, ticket = validate_record(record)

        if is_valid:
            if valid_producer:
                valid_producer.send(TOPIC_VALID, value=record)
            valid_count += 1
        else:
            if dlq_producer:
                quarantine_record = {**record, "_rejection_reason": rejection_reason}
                dlq_producer.send(TOPIC_DLQ, value=quarantine_record)
            if quarantine_callback:
                quarantine_callback(record, rejection_reason)
            invalid_count += 1
            dlq_count += 1

    if valid_producer:
        valid_producer.flush()
    if dlq_producer:
        dlq_producer.flush()

    return {"valid": valid_count, "invalid": invalid_count, "dlq_routed": dlq_count}
