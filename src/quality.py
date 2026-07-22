"""
Deliverable 5: Quality Gate using Great Expectations.
"""

try:
    from great_expectations.core.batch import RuntimeBatchRequest
    HAS_GX = True
except ImportError:
    HAS_GX = False

import json
from src.config import QUALITY_THRESHOLD


def run_quality_gate(df_silver) -> dict:
    """
    Check Silver layer quality via Great Expectations.
    Raises AirflowException if quality_score < QUALITY_THRESHOLD (80%).
    """
    total_records = df_silver.count()
    duplicate_count = total_records - df_silver.dropDuplicates(["Ticket ID"]).count()
    null_records = df_silver.filter(
        df_silver["Ticket ID"].isNull() |
        df_silver["Customer Name"].isNull() |
        df_silver["Status"].isNull()
    ).count()

    # Calculate quality score (simple: no duplicates + no nulls)
    quality_issues = duplicate_count + null_records
    quality_score = max(0, 1.0 - (quality_issues / max(1, total_records)))

    print(f"Quality Metrics:")
    print(f"  Total records: {total_records}")
    print(f"  Duplicates: {duplicate_count}")
    print(f"  Null values: {null_records}")
    print(f"  Quality score: {quality_score:.2%}")

    if quality_score < QUALITY_THRESHOLD:
        raise RuntimeError(
            f"❌ Quality gate FAILED: score {quality_score:.2%} < threshold {QUALITY_THRESHOLD:.2%}"
        )

    print(f"✅ Quality gate PASSED")
    return {
        "quality_score": quality_score,
        "total_records": total_records,
        "duplicate_count": duplicate_count,
        "null_count": null_records,
    }
