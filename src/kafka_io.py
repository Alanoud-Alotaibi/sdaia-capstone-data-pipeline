"""
Deliverable 1: Ingestion via Kafka with Pydantic schema validation.
Kafka producer/consumer with dead-letter queue routing and quarantine file logging.
"""

import json
import os
from typing import Optional, Dict, Any, List, Tuple
from pydantic import BaseModel, Field, field_validator, ValidationError

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

from src.config import KAFKA_BOOTSTRAP, TOPIC_RAW, TOPIC_VALID, TOPIC_DLQ, QUARANTINE_PATH


class TicketRecord(BaseModel):
    """Pydantic data contract for inbound CRM support tickets."""
    ticket_id: str = Field(..., alias="Ticket_ID", description="Unique ticket identifier")
    customer_name: str = Field(..., alias="Customer_Name", description="Customer full name")
    customer_email: Optional[str] = Field(default=None, alias="Customer_Email")
    ticket_subject: Optional[str] = Field(default=None, alias="Ticket_Subject")
    ticket_description: Optional[str] = Field(default=None, alias="Ticket_Description")
    issue_category: Optional[str] = Field(default=None, alias="Issue_Category")
    priority_level: Optional[str] = Field(default=None, alias="Priority_Level")
    ticket_channel: Optional[str] = Field(default=None, alias="Ticket_Channel")
    submission_date: Optional[str] = Field(default=None, alias="Submission_Date")
    resolution_time_hours: Optional[float] = Field(default=None, alias="Resolution_Time_Hours")
    assigned_agent: Optional[str] = Field(default=None, alias="Assigned_Agent")
    satisfaction_score: Optional[float] = Field(default=None, alias="Satisfaction_Score")

    @field_validator("ticket_id", "customer_name", mode="before")
    def check_not_empty(cls, v, info):
        if v is None or str(v).strip() == "" or str(v).lower() == "none" or str(v).lower() == "nan":
            raise ValueError(f"Field '{info.field_name}' cannot be empty or null")
        return str(v).strip()

    @field_validator("satisfaction_score", mode="before")
    def check_satisfaction_score(cls, v):
        if v is None or v == "" or str(v).lower() == "nan":
            return None
        try:
            val = float(v)
            if val < 1.0 or val > 5.0:
                raise ValueError(f"Satisfaction score must be between 1.0 and 5.0 (got {val})")
            return val
        except (ValueError, TypeError):
            raise ValueError(f"Invalid satisfaction score: {v}")

    @field_validator("priority_level", mode="before")
    def check_priority(cls, v):
        if v is None or str(v).strip() == "":
            return "Medium"
        valid_priorities = ["Low", "Medium", "High", "Critical"]
        val = str(v).strip()
        if val not in valid_priorities:
            raise ValueError(f"Priority_Level '{val}' not in allowed values: {valid_priorities}")
        return val

    class Config:
        populate_by_name = True
        extra = "ignore"


def get_producer() -> Optional[Any]:
    """Initialize Kafka Producer or return None if broker unreachable."""
    if not HAS_KAFKA:
        return None
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            request_timeout_ms=3000,
        )
        return producer
    except Exception as e:
        print(f"[WARN] Kafka producer unavailable: {e}")
        return None


def get_consumer(topic: str) -> Optional[Any]:
    """Initialize Kafka Consumer for given topic or return None."""
    if not HAS_KAFKA:
        return None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=3000,
        )
        return consumer
    except Exception as e:
        print(f"[WARN] Kafka consumer unavailable for topic '{topic}': {e}")
        return None


def validate_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """
    Validate a dictionary record against TicketRecord Pydantic schema.
    Returns: (is_valid, rejection_reason, validated_dict)
    """
    try:
        normalized = {}
        for k, v in record.items():
            normalized[k] = v

        validated = TicketRecord(**normalized)
        return True, None, validated.model_dump(by_alias=True)
    except ValidationError as e:
        rejection_reasons = []
        for err in e.errors():
            field = ".".join(str(loc) for loc in err["loc"])
            rejection_reasons.append(f"{field}: {err['msg']}")
        reason = " | ".join(rejection_reasons)
        return False, reason, None
    except Exception as e:
        return False, str(e), None


def produce_records(producer: Any, records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Produce records to TOPIC_RAW."""
    if not producer:
        return {"produced": 0, "failed": len(records), "status": "producer_unavailable"}

    produced, failed = 0, 0
    for record in records:
        try:
            future = producer.send(TOPIC_RAW, value=record)
            future.get(timeout=5)
            produced += 1
        except Exception:
            failed += 1

    producer.flush()
    return {"produced": produced, "failed": failed, "status": "success"}


def consume_and_validate(records_input: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Consume records from Kafka TOPIC_RAW (or fallback input list),
    validate each against Pydantic schema, route valid to TOPIC_VALID,
    and invalid to TOPIC_DLQ + local quarantine file.
    """
    consumer = get_consumer(TOPIC_RAW)
    valid_producer = get_producer()
    dlq_producer = get_producer()

    raw_records = []
    if consumer:
        try:
            for message in consumer:
                raw_records.append(message.value)
        except Exception as e:
            print(f"[WARN] Error reading from Kafka consumer: {e}")

    if not raw_records and records_input:
        raw_records = records_input

    valid_records = []
    invalid_records = []

    os.makedirs(os.path.dirname(QUARANTINE_PATH), exist_ok=True)

    with open(QUARANTINE_PATH, "a", encoding="utf-8") as q_file:
        for rec in raw_records:
            is_valid, rejection_reason, validated_dict = validate_record(rec)
            if is_valid and validated_dict:
                valid_records.append(validated_dict)
                if valid_producer:
                    try:
                        valid_producer.send(TOPIC_VALID, value=validated_dict)
                    except Exception:
                        pass
            else:
                quarantine_entry = {
                    "raw_record": rec,
                    "rejection_reason": rejection_reason,
                    "status": "QUARANTINED"
                }
                invalid_records.append(quarantine_entry)
                q_file.write(json.dumps(quarantine_entry) + "\n")
                if dlq_producer:
                    try:
                        dlq_producer.send(TOPIC_DLQ, value=quarantine_entry)
                    except Exception:
                        pass

    if valid_producer:
        valid_producer.flush()
    if dlq_producer:
        dlq_producer.flush()

    return {
        "total_processed": len(raw_records),
        "valid_count": len(valid_records),
        "invalid_count": len(invalid_records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "quarantine_file": QUARANTINE_PATH,
    }
