"""
Deliverable 4: Airflow DAG orchestration.
TaskFlow API with correct dependencies and quality gate halt.
"""

try:
    from airflow import DAG
    from airflow.decorators import task, dag
    from airflow.utils.dates import days_ago
    HAS_AIRFLOW = True
except ImportError:
    HAS_AIRFLOW = False


@dag(dag_id="capstone_dag", start_date=days_ago(0), schedule_interval=None, catchup=False)
def capstone_dag():
    """
    Full capstone pipeline DAG.
    Dependency chain: produce → validate → bronze → silver → quality_gate → gold → rag
    If quality_gate fails, downstream tasks halt.
    """
    from src.tasks import (
        task_produce, task_consume_validate, task_bronze, task_silver,
        task_quality_gate, task_gold, task_rag
    )
    from src.synthetic_data import generate_synthetic_tickets

    # Generate synthetic records
    records = generate_synthetic_tickets(1000, bad_row_fraction=0.05).to_dict("records")

    # Task 1: Produce
    produce_result = task_produce(records)

    # Task 2: Consume & Validate
    validate_result = task_consume_validate()

    # Task 3: Bronze
    bronze_result = task_bronze("./data/raw/tickets.csv")

    # Task 4: Silver
    silver_result = task_silver()

    # Task 5: Quality Gate (BLOCKS if fails)
    quality_result = task_quality_gate()

    # Task 6: Gold (only runs if quality passes)
    gold_result = task_gold()

    # Task 7: RAG (only runs if quality passes)
    rag_result = task_rag()

    # Dependencies
    produce_result >> validate_result
    validate_result >> bronze_result >> silver_result >> quality_result
    quality_result >> gold_result >> rag_result


def run_pipeline():
    """Run the pipeline end-to-end (for non-Airflow execution)."""
    print("🚀 Starting SDAIA Capstone Pipeline...\n")

    from src.tasks import (
        task_produce, task_consume_validate, task_bronze, task_silver,
        task_quality_gate, task_gold, task_rag
    )
    from src.synthetic_data import generate_synthetic_tickets

    try:
        # 1. Produce
        print("Step 1: Produce records to Kafka")
        records = generate_synthetic_tickets(100, bad_row_fraction=0.05).to_dict("records")
        result1 = task_produce(records)
        print(result1, "\n")

        # 2. Validate
        print("Step 2: Consume & validate")
        result2 = task_consume_validate()
        print(result2, "\n")

        # 3. Bronze
        print("Step 3: Load Bronze layer")
        result3 = task_bronze("./data/raw/tickets.csv")
        print(result3, "\n")

        # 4. Silver
        print("Step 4: MERGE into Silver")
        result4 = task_silver()
        print(result4, "\n")

        # 5. Quality Gate
        print("Step 5: Quality Gate")
        result5 = task_quality_gate()
        print(result5, "\n")

        # 6. Gold
        print("Step 6: Build Gold layer")
        result6 = task_gold()
        print(result6, "\n")

        # 7. RAG
        print("Step 7: RAG Pipeline")
        result7 = task_rag()
        print(result7, "\n")

        print("✅ Pipeline completed successfully!")

    except RuntimeError as e:
        print(f"❌ Pipeline failed: {e}")


# For Airflow, instantiate the DAG
if HAS_AIRFLOW:
    dag_instance = capstone_dag()
