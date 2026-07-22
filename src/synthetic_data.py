"""
Deliverable 1 (cont.): Synthetic data generation for testing.
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from typing import Optional


def generate_synthetic_tickets(n: int, bad_row_fraction: float = 0.05) -> pd.DataFrame:
    """
    Generate synthetic CRM ticket dataset.
    Args:
        n: Number of tickets to generate
        bad_row_fraction: Fraction of records to intentionally malform
    Returns:
        DataFrame with columns: Ticket ID, Customer Name, Priority, Status, ...
    """
    statuses = ["Open", "In Progress", "Resolved", "Closed", "On Hold"]
    priorities = ["Low", "Medium", "High", "Critical"]
    customers = [f"Customer_{i}" for i in range(100)]

    records = []
    for i in range(n):
        record = {
            "Ticket ID": f"TKT-{i:06d}",
            "Customer Name": random.choice(customers),
            "Priority": random.choice(priorities),
            "Status": random.choice(statuses),
            "Satisfaction score": round(random.uniform(1, 5), 1),
            "Resolution time": random.randint(1, 720),  # minutes
            "Description": f"Issue #{i}: Sample ticket description.",
        }
        records.append(record)

    df = pd.DataFrame(records)

    # Inject malformed records
    bad_count = int(n * bad_row_fraction)
    if bad_count > 0:
        df = _inject_malformed_rows(df, random, bad_row_fraction)

    return df


def _inject_malformed_rows(df: pd.DataFrame, random_module, fraction: float) -> pd.DataFrame:
    """Inject malformed rows (missing fields, wrong types, etc.)"""
    n_bad = int(len(df) * fraction)
    bad_indices = random_module.sample(range(len(df)), k=n_bad)

    for idx in bad_indices:
        malform_type = random_module.choice(["missing_id", "missing_name", "bad_score"])
        if malform_type == "missing_id":
            df.loc[idx, "Ticket ID"] = None
        elif malform_type == "missing_name":
            df.loc[idx, "Customer Name"] = None
        elif malform_type == "bad_score":
            df.loc[idx, "Satisfaction score"] = "invalid"

    return df
