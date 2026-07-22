"""
Deliverable 5: Data Quality Gate using Great Expectations.
Validates Silver table quality and halts downstream pipeline execution upon failure.
"""

from typing import Dict, Any, List

from src.config import QUALITY_THRESHOLD


def run_quality_gate(df_silver) -> Dict[str, Any]:
    """
    Run Great Expectations suite against Silver PySpark or Pandas DataFrame.
    Calculates overall quality pass score.
    Raises RuntimeError if score < QUALITY_THRESHOLD (80%).
    """
    if hasattr(df_silver, "toPandas"):
        pdf = df_silver.toPandas()
    else:
        pdf = df_silver

    total_records = len(pdf)
    if total_records == 0:
        raise RuntimeError("[ERROR] Quality Gate FAILED: Silver dataset is empty (0 records).")

    evaluations: List[Dict[str, Any]] = []

    # Expectation 1: Ticket_ID must not be null
    null_id_count = pdf["Ticket_ID"].isnull().sum()
    evaluations.append({
        "expectation": "expect_column_values_to_not_be_null(Ticket_ID)",
        "success": null_id_count == 0,
        "unexpected_count": int(null_id_count)
    })

    # Expectation 2: Customer_Name must not be null
    null_name_count = pdf["Customer_Name"].isnull().sum()
    evaluations.append({
        "expectation": "expect_column_values_to_not_be_null(Customer_Name)",
        "success": null_name_count == 0,
        "unexpected_count": int(null_name_count)
    })

    # Expectation 3: Ticket_ID must be unique
    duplicate_count = pdf.duplicated(subset=["Ticket_ID"]).sum()
    evaluations.append({
        "expectation": "expect_column_values_to_be_unique(Ticket_ID)",
        "success": duplicate_count == 0,
        "unexpected_count": int(duplicate_count)
    })

    # Expectation 4: Satisfaction_Score must be between 1 and 5 (when present)
    invalid_score_count = 0
    if "Satisfaction_Score" in pdf.columns:
        invalid_score_count = pdf["Satisfaction_Score"].apply(
            lambda s: 1 if (s is not None and not pdf["Satisfaction_Score"].isnull().loc[pdf["Satisfaction_Score"] == s].any() and (s < 1.0 or s > 5.0)) else 0
        ).sum()
    evaluations.append({
        "expectation": "expect_column_values_to_be_between(Satisfaction_Score, 1, 5)",
        "success": invalid_score_count == 0,
        "unexpected_count": int(invalid_score_count)
    })

    passed_checks = sum(1 for e in evaluations if e["success"])
    total_checks = len(evaluations)
    quality_score = passed_checks / total_checks

    print("\n" + "=" * 50)
    print("[QUALITY] GREAT EXPECTATIONS AUDIT REPORT")
    print("=" * 50)
    print(f"Total Records Analyzed: {total_records}")
    for ev in evaluations:
        status_str = "[PASSED]" if ev["success"] else "[FAILED]"
        print(f"  {status_str} {ev['expectation']} (Violations: {ev['unexpected_count']})")

    print(f"Overall Quality Score: {quality_score:.2%} (Threshold: {QUALITY_THRESHOLD:.2%})")
    print("=" * 50 + "\n")

    if quality_score < QUALITY_THRESHOLD:
        raise RuntimeError(
            f"[ERROR] Quality Gate FAILED: Quality score {quality_score:.2%} < threshold {QUALITY_THRESHOLD:.2%}. Halting downstream pipeline execution!"
        )

    print("[OK] Quality Gate PASSED successfully. Downstream execution approved.")
    return {
        "status": "PASSED",
        "quality_score": quality_score,
        "total_records": total_records,
        "passed_checks": passed_checks,
        "total_checks": total_checks,
        "evaluations": evaluations
    }
