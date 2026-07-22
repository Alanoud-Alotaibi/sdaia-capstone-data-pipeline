"""
Configuration module for SDAIA Capstone.
Centralized configuration for paths, Kafka, environment detection, and quality settings.
"""

import os
import sys

# Detect environment
IN_COLAB = "google.colab" in sys.modules
IN_WINDOWS = sys.platform == "win32"

# Data paths
if IN_COLAB:
    DATA_BASE = "/content/capstone-data"
else:
    DATA_BASE = "./data"

RAW_DIR = os.path.join(DATA_BASE, "raw")
DELTA_DIR = os.path.join(DATA_BASE, "delta")
CHROMA_DIR = os.path.join(DATA_BASE, "chroma")
BM25_INDEX_PATH = os.path.join(DATA_BASE, "bm25_index.pkl")
QUARANTINE_PATH = os.path.join(DATA_BASE, "quarantine", "quarantine.jsonl")
LINEAGE_PATH = os.path.join(DATA_BASE, "lineage_events.jsonl")

# Ensure required directories exist
for path in [RAW_DIR, DELTA_DIR, CHROMA_DIR, os.path.dirname(QUARANTINE_PATH), os.path.dirname(LINEAGE_PATH)]:
    os.makedirs(path, exist_ok=True)

# Kafka configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = "support-tickets-raw"
TOPIC_VALID = "support-tickets-valid"
TOPIC_DLQ = "support-tickets-dlq"

# Column mapping (aligned 1:1 with Kaggle Customer Support Tickets CRM Dataset)
COLUMN_MAP = {
    "Ticket_ID": "ticket_id",
    "Customer_Name": "customer_name",
    "Customer_Email": "customer_email",
    "Ticket_Subject": "ticket_subject",
    "Ticket_Description": "ticket_description",
    "Issue_Category": "issue_category",
    "Priority_Level": "priority_level",
    "Ticket_Channel": "ticket_channel",
    "Submission_Date": "submission_date",
    "Resolution_Time_Hours": "resolution_time_hours",
    "Assigned_Agent": "assigned_agent",
    "Satisfaction_Score": "satisfaction_score",
}

# Quality thresholds
QUALITY_THRESHOLD = 0.80  # 80% minimum quality score required
MAX_DUPLICATES_ALLOWED = 50
MAX_NULL_RECORDS_ALLOWED = 20

# RAG configuration
RAG_CHUNK_SIZE = 400
RAG_CHUNK_OVERLAP = 80
RAG_TOP_K = 5
CROSS_ENCODER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"

print(f"[CONFIG] Environment: IN_COLAB={IN_COLAB}, IN_WINDOWS={IN_WINDOWS}")
print(f"[CONFIG] Base Data Path: {os.path.abspath(DATA_BASE)}")
