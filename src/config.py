"""
Configuration module for SDAIA Capstone.
Centralized configuration for paths, Kafka, environment detection.
"""

import os
import sys

# Detect environment
IN_COLAB = "google.colab" in sys.modules
IN_WINDOWS = sys.platform == "win32"

# Data paths
if IN_COLAB:
    RAW_DIR = "/content/capstone-data/raw"
    DELTA_DIR = "/content/capstone-data/delta"
    CHROMA_DIR = "/content/capstone-data/chroma"
    BM25_INDEX_PATH = "/content/capstone-data/bm25_index.pkl"
    QUARANTINE_PATH = "/content/capstone-data/quarantine/quarantine.jsonl"
    LINEAGE_PATH = "/content/capstone-data/lineage_events.jsonl"
else:
    RAW_DIR = "./data/raw"
    DELTA_DIR = "./data/delta"
    CHROMA_DIR = "./data/chroma"
    BM25_INDEX_PATH = "./data/bm25_index.pkl"
    QUARANTINE_PATH = "./data/quarantine/quarantine.jsonl"
    LINEAGE_PATH = "./data/lineage_events.jsonl"

# Create directories
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(DELTA_DIR, exist_ok=True)
os.makedirs(CHROMA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(QUARANTINE_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LINEAGE_PATH), exist_ok=True)

# Kafka configuration
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RAW = "support-tickets-raw"
TOPIC_VALID = "support-tickets-valid"
TOPIC_DLQ = "support-tickets-dlq"

# Column mapping (for Kaggle CRM dataset)
COLUMN_MAP = {
    "Ticket ID": "ticket_id",
    "Customer Name": "customer_name",
    "Priority": "priority",
    "Status": "status",
    "Satisfaction score": "satisfaction_score",
    "Resolution time": "resolution_time",
    "Description": "description",
}

# Quality thresholds
QUALITY_THRESHOLD = 0.80  # 80% quality score required
MAX_DUPLICATES = 200
MAX_NULL_RECORDS = 50

# RAG configuration
RAG_CHUNK_SIZE = 500
RAG_CHUNK_OVERLAP = 100
RAG_TOP_K = 5

print(f"Config loaded: IN_COLAB={IN_COLAB}, IN_WINDOWS={IN_WINDOWS}")
print(f"Paths: RAW={RAW_DIR}, DELTA={DELTA_DIR}")
