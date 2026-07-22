"""
Task definitions for pipeline orchestration.
Encapsulates pipeline tasks wrapped with OpenLineage stage tracing context managers.
"""

import os
import pandas as pd
from typing import List, Dict, Any
from src.lineage import traced_stage
from src.config import DELTA_DIR


def task_produce(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Task 1: Ingest & Produce records to Kafka TOPIC_RAW."""
    with traced_stage("produce", inputs=["raw_crm_dataset"], outputs=["support-tickets-raw"]):
        from src.kafka_io import produce_records, get_producer
        producer = get_producer()
        result = produce_records(producer, records)
        return result


def task_consume_validate(records_fallback: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Task 2: Consume records, validate with Pydantic schema, route to VALID or DLQ."""
    with traced_stage("consume_validate", inputs=["support-tickets-raw"], outputs=["support-tickets-valid", "support-tickets-dlq", "quarantine_jsonl"]):
        from src.kafka_io import consume_and_validate
        result = consume_and_validate(records_input=records_fallback)
        return result


def task_bronze(valid_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Task 3: Load valid records into Delta Bronze layer."""
    with traced_stage("bronze", inputs=["support-tickets-valid"], outputs=["delta_bronze"]):
        from src.lakehouse import get_spark, load_bronze
        spark = get_spark()
        bronze_res = load_bronze(spark, valid_records)
        count_val = len(bronze_res) if hasattr(bronze_res, "__len__") else bronze_res.count()
        return {"status": "SUCCESS", "bronze_count": count_val}


def task_silver() -> Dict[str, Any]:
    """Task 4: MERGE (upsert) Bronze into Silver Delta table on Ticket_ID."""
    with traced_stage("silver", inputs=["delta_bronze"], outputs=["delta_silver"]):
        from src.lakehouse import get_spark, merge_silver, verify_schema_enforcement
        spark = get_spark()
        silver_res = merge_silver(spark)
        schema_ok = verify_schema_enforcement(spark)
        count_val = len(silver_res) if hasattr(silver_res, "__len__") else silver_res.count()
        return {"status": "SUCCESS", "silver_count": count_val, "schema_enforcement_verified": schema_ok}


def task_quality_gate() -> Dict[str, Any]:
    """Task 5: Execute Great Expectations quality gate on Silver layer. Halts if failed."""
    with traced_stage("quality_gate", inputs=["delta_silver"], outputs=["quality_audit_metrics"]):
        from src.lakehouse import get_spark
        from src.quality import run_quality_gate
        spark = get_spark()
        silver_path = os.path.join(DELTA_DIR, "silver")

        if spark:
            silver_df = spark.read.format("delta").load(silver_path)
        else:
            silver_file = os.path.join(silver_path, "silver.csv")
            silver_df = pd.read_csv(silver_file) if os.path.exists(silver_file) else pd.DataFrame()

        metrics = run_quality_gate(silver_df)
        return metrics


def task_gold() -> Dict[str, Any]:
    """Task 6: Build Gold layer business metrics tables."""
    with traced_stage("gold", inputs=["delta_silver"], outputs=["delta_gold_category", "delta_gold_agent", "delta_gold_sla"]):
        from src.lakehouse import get_spark, build_gold
        spark = get_spark()
        gold_tables = build_gold(spark)
        cat_cnt = len(gold_tables["gold_category"]) if hasattr(gold_tables["gold_category"], "__len__") else gold_tables["gold_category"].count()
        agt_cnt = len(gold_tables["gold_agent"]) if hasattr(gold_tables["gold_agent"], "__len__") else gold_tables["gold_agent"].count()
        sla_cnt = len(gold_tables["gold_sla"]) if hasattr(gold_tables["gold_sla"], "__len__") else gold_tables["gold_sla"].count()

        return {
            "status": "SUCCESS",
            "gold_category_count": cat_cnt,
            "gold_agent_count": agt_cnt,
            "gold_sla_count": sla_cnt
        }


def task_rag(query: str = "Hours of operation inquiry") -> Dict[str, Any]:
    """Task 7: Build Hybrid RAG Pipeline & Execute Search with Citations."""
    with traced_stage("rag", inputs=["delta_gold_category"], outputs=["chroma_vector_db", "bm25_index", "rag_answer"]):
        from src.lakehouse import get_spark
        from src.rag import chunk_documents, build_vector_and_bm25_indices, answer_query_with_citations

        spark = get_spark()
        silver_path = os.path.join(DELTA_DIR, "silver")

        if spark:
            silver_df = spark.read.format("delta").load(silver_path)
        else:
            silver_file = os.path.join(silver_path, "silver.csv")
            silver_df = pd.read_csv(silver_file) if os.path.exists(silver_file) else pd.DataFrame()

        documents = chunk_documents(silver_df)
        chroma_col, bm25_idx = build_vector_and_bm25_indices(documents)
        rag_response = answer_query_with_citations(query, documents, chroma_col, bm25_idx)

        return {
            "status": "SUCCESS",
            "documents_count": len(documents),
            "query": query,
            "answer": rag_response["answer"],
            "citations_count": len(rag_response["citations"])
        }
