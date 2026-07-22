"""
Deliverable 1 (cont.): Synthetic data generation for testing.
Generates CRM support ticket records matching customer_support_tickets.csv schema.
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any


def generate_synthetic_tickets(n: int = 100, bad_row_fraction: float = 0.05) -> pd.DataFrame:
    """
    Generate synthetic CRM ticket dataset matching customer_support_tickets.csv.
    
    Args:
        n: Total number of tickets to generate
        bad_row_fraction: Fraction of records to intentionally malform for DLQ/quality gate testing
        
    Returns:
        DataFrame with dataset columns
    """
    categories = ["Technical", "Account", "Billing", "General Inquiry", "Product Feedback"]
    priorities = ["Low", "Medium", "High", "Critical"]
    channels = ["Web Form", "Chat", "Email", "Phone"]
    agents = ["David Kim", "Elena Rodriguez", "Anya Sharma", "Marcus Vance", "Sarah Jenkins"]
    subjects = [
        "Hours of operation inquiry",
        "Data not syncing across devices",
        "2FA login authentication failure",
        "Subscription billing charge question",
        "Export CSV function throwing error"
    ]

    records: List[Dict[str, Any]] = []
    base_date = datetime(2025, 1, 1)

    for i in range(n):
        sub_date = (base_date + timedelta(days=random.randint(0, 200))).strftime("%Y-%m-%d")
        record = {
            "Ticket_ID": f"TKT-{100000 + i}",
            "Customer_Name": f"Customer_{i}",
            "Customer_Email": f"user{i}@example.com",
            "Ticket_Subject": random.choice(subjects),
            "Ticket_Description": f"Hi Support, experiencing issue #{i} with system. Need resolution quickly.",
            "Issue_Category": random.choice(categories),
            "Priority_Level": random.choice(priorities),
            "Ticket_Channel": random.choice(channels),
            "Submission_Date": sub_date,
            "Resolution_Time_Hours": random.randint(1, 72),
            "Assigned_Agent": random.choice(agents),
            "Satisfaction_Score": random.randint(1, 5),
        }
        records.append(record)

    df = pd.DataFrame(records)

    # Inject malformed rows for DLQ & Quality Gate failure tests
    bad_count = int(n * bad_row_fraction)
    if bad_count > 0:
        bad_indices = random.sample(range(len(df)), k=bad_count)
        for idx in bad_indices:
            malform_type = random.choice(["missing_id", "missing_name", "bad_score", "invalid_priority"])
            if malform_type == "missing_id":
                df.loc[idx, "Ticket_ID"] = None
            elif malform_type == "missing_name":
                df.loc[idx, "Customer_Name"] = None
            elif malform_type == "bad_score":
                df.loc[idx, "Satisfaction_Score"] = 99  # Invalid score > 5
            elif malform_type == "invalid_priority":
                df.loc[idx, "Priority_Level"] = "Urgent_NonStandard"

    return df
