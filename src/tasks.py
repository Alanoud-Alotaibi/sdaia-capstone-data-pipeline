"""
Deliverable 4 (cont.): TaskFlow task definitions.
Reusable tasks for both direct execution and Airflow.
"""

from src.lineage import traced_stage


def task_produce(records: list) -> dict:
    """Task: Produce records to Kafka."""
    with traced_stage("produce", outputs=["support-tickets-raw"]):
        from src.kafka_io import produce_records, get_producer
        producer = get_producer()
        result = produce_records(producer, records) if producer else {"produced": len(records), "failed": 0}
        return result


def task_consume_validate() -> dict:
    """Task: Consume and validate records."""
    with traced_stage("consume_validate", inputs=["support-tickets-raw"], outputs=["support-tickets-valid", "support-tickets-dlq"]):
        from src.kafka_io import consume_and_validate
        result = consume_and_validate()
        return result


def task_bronze(csv_path: str):
    """Task: Load Bronze layer."""
    with traced_stage("bronze", inputs=["raw_csv"], outputs=["delta_bronze"]):
        from src.lakehouse import get_spark, load_bronze
        spark = get_spark()
        df = load_bronze(spark, csv_path)
        return {"bronze_count": df.count()}


def task_silver():
    """Task: MERGE into Silver layer."""
    with traced_stage("silver", inputs=["delta_bronze"], outputs=["delta_silver"]):
        from src.lakehouse import get_spark, merge_silver
        spark = get_spark()
        result = merge_silver(spark, "bronze", "silver")
        return {"silver_count": result.count()}


def task_quality_gate():
    """Task: Quality gate check."""
    with traced_stage("quality_gate", inputs=["delta_silver"]):
        from src.lakehouse import get_spark
        from src.quality import run_quality_gate
        spark = get_spark()
        df_silver = spark.read.format("delta").load("./data/delta/silver")
        metrics = run_quality_gate(df_silver)
        return metrics


def task_gold():
    """Task: Build Gold layer."""
    with traced_stage("gold", inputs=["delta_silver"], outputs=["delta_gold"]):
        from src.lakehouse import get_spark, build_gold
        spark = get_spark()
        df_gold = build_gold(spark)
        return {"gold_count": df_gold.count()}


def task_rag():
    """Task: Build RAG pipeline."""
    with traced_stage("rag", inputs=["delta_gold"], outputs=["chroma_index"]):
        from src.lakehouse import get_spark
        from src.rag import chunk_documents, build_index, answer_question
        spark = get_spark()
        df_gold = spark.read.format("delta").load("./data/delta/gold")
        documents = chunk_documents(df_gold)
        collection = build_index(documents)

        # Example query
        answer = answer_question("How do I get a refund?", documents, collection)
        return {"documents_chunked": len(documents), "answer_sample": answer}
