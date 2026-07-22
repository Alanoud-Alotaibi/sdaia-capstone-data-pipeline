"# SDAIA Capstone: Modern Data Engineering for AI System on CRM Dataset

##  Project Overview

**Program:** SDAIA Academy Data Engineering Track  

This capstone project integrates **5 core data engineering modules** into a production-ready data pipeline for processing customer support tickets. The system demonstrates enterprise-grade data practices including real-time ingestion, data quality validation, lakehouse architecture, retrieval-augmented generation (RAG), and orchestration.

##  Problem Statement

Organizations struggle to process customer support tickets at scale while maintaining:
- **Data Quality:** Validation and rejection of malformed records
- **Scalability:** Real-time processing via message brokers
- **Analytics:** Historical data storage with time-travel capabilities
- **Intelligence:** AI-powered semantic search and retrieval
- **Reliability:** Automated orchestration with failure gates

##  Architecture

```
Customer Tickets (Raw Data)
         ↓
    ┌──KAFKA────────────────┐
    │ Producer & Consumer   │
    │ (kafka-python)        │
    └──────────────────────┘
         ↓
    Validation (Pydantic)
         ├─→ Valid Data → TOPIC_VALID
         └─→ Malformed → TOPIC_DLQ → Quarantine Zone
         
    Validated Data
         ↓
    ┌─────────────────────────────────────┐
    │   Delta Lakehouse (delta-spark)     │
    ├─────────────────────────────────────┤
    │ Bronze: Raw + Metadata              │
    │ Silver: Deduplicated + MERGE Upsert │
    │ Gold: Business Aggregates           │
    └─────────────────────────────────────┘
         ↓
    Quality Gates (great-expectations)
         ├─→ ✅ PASS → Continue
         └─→ ❌ FAIL → AirflowException (halt downstream)
         
    ┌─────────────────────────────────────┐
    │   RAG Pipeline                      │
    ├─────────────────────────────────────┤
    │ • Document Chunking                 │
    │ • Vector Embeddings (Chroma)        │
    │ • BM25 Index (rank-bm25)            │
    │ • Hybrid Search + Reranking         │
    │ • Grounded QA with Citations        │
    └─────────────────────────────────────┘
         ↓
    Orchestration (apache-airflow)
         ↓
    Lineage Tracking (openlineage-python)
```
##  Prerequisites

- Python 3.8+
- Java Runtime (for PySpark)
- Kafka (optional; file-based fallback included)
- Git

##  Installation

### 1. Clone Repository
```bash
git clone https://github.com/SDAIAAcademy/sdaia-capstone-data-pipeline.git
cd sdaia-capstone-data-pipeline
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -q \
    pyspark==3.5.0 delta-spark==3.2.0 \
    kafka-python==2.0.2 "pydantic>=2.0" \
    kagglehub chromadb sentence-transformers rank-bm25 \
    "apache-airflow>=2.8" "great-expectations>=1.0" openlineage-python
```

### 4. Set Environment Variables (Optional)
```bash
# For Kaggle dataset access
export KAGGLE_USERNAME=<your_username>
export KAGGLE_KEY=<your_api_key>
```

---

##  Running the Pipeline

### Option A: Jupyter Notebook (Recommended)
```bash
jupyter notebook "Modern Data Engineering for AI System - On CRM Dataset.ipynb"
# Run all cells (Ctrl+Shift+Enter) for complete execution
```

### Option B: Airflow DAG
```bash
# Initialize database
airflow db init

# Start webserver
airflow webserver -p 8080

# Trigger DAG in another terminal
airflow dags trigger capstone_dag
```

### Option C: Full Pipeline Run
```bash
python -c "
import sys; sys.path.insert(0, '.')
from src.dag_pipeline import run_pipeline
run_pipeline()
"
```

---

##  Project Structure

```
.
├── Modern Data Engineering for AI System - On CRM Dataset.ipynb
├── README.md
├── requirements.txt
├── .gitignore
└── src/
    ├── __init__.py
    ├── config.py                 # Configuration (paths, Kafka, env)
    ├── kafka_io.py               # Producer/Consumer (kafka-python)
    ├── synthetic_data.py         # Test data generation
    ├── tasks.py                  # TaskFlow tasks
    ├── lakehouse.py              # Delta operations (delta-spark)
    ├── quality.py                # Quality checks (great-expectations)
    ├── lineage.py                # Lineage events (openlineage-python)
    └── rag.py                    # RAG pipeline (chromadb, BM25)
```

---

##  Key Features

### 1️⃣ **Ingestion**
- ✅ Real **kafka-python** producer/consumer
- ✅ **Pydantic** schema validation
- ✅ Dead-letter topic routing
- ✅ Malformed records logged with rejection reason
- **Proof:** Cell 24 output shows DLQ routing

### 2️⃣ **Delta Lakehouse**
- ✅ Bronze layer: Raw + ingestion metadata
- ✅ Silver layer: MERGE upsert on ticket_id (business key)
- ✅ Schema enforcement (Delta validates writes)
- ✅ Gold layer: Real aggregates (not just copies)
- **Proof:** Cells 16-21 show delta-spark MERGE operations

### 3️⃣ **RAG Pipeline**
- ✅ Document chunking (500 char + overlap)
- ✅ Vector embeddings (sentence-transformers → chromadb)
- ✅ BM25 keyword index (rank-bm25)
- ✅ Hybrid search (vector + BM25 combined)
- ✅ Reranking (cross-encoder)
- ✅ Grounded answers with citations
- **Proof:** Cells 40-43 demonstrate query examples

### 4️⃣ **Orchestration**
- ✅ Airflow TaskFlow API (@task decorators)
- ✅ Correct dependencies (produce → validate → bronze → silver → gold → quality_gate → rag)
- ✅ Quality gate halts downstream on failure
- ✅ Lineage events emitted (START/COMPLETE/FAIL)
- **Proof:** Cell 21 defines DAG with task dependencies

### 5️⃣ **Quality + Lineage**
- ✅ Great Expectations checks (schema, duplicates, nulls)
- ✅ Quality gate blocks pipeline if score < 80%
- ✅ OpenLineage events recorded
- ✅ Failure paths demonstrated (quarantine zone, schema violations)
- **Proof:** Cells 34-36 show quality metrics + lineage

---

##  Required Libraries (Real Implementations)

| Component | Library | Version | Status |
|---|---|---|---|
| Message Queue | kafka-python | ≥ 2.0.2 | ✅ |
| DataFrame | pyspark | ≥ 3.5.0 | ✅ |
| Lakehouse | delta-spark | ≥ 3.2.0 | ✅ |
| Vector Store | chromadb | ≥ 0.4.0 | ✅ |
| Embeddings | sentence-transformers | ≥ 2.0.0 | ✅ |
| Keyword Search | rank-bm25 | ≥ 0.2.1 | ✅ |
| Validation | pydantic | ≥ 2.0 | ✅ |
| Quality | great-expectations | ≥ 1.0 | ✅ |
| Orchestration | apache-airflow | ≥ 2.8 | ✅ |
| Lineage | openlineage-python | Latest | ✅ |

**No Simulations:** All components use real production libraries. File-based fallback used only for Kafka on Windows (environment detection).

---

##  Expected Output

### ✅ Successful Run
```
Step 1: Ingestion
✅ Produced 3000 records to support-tickets-raw topic
✅ Validated 2850 records → support-tickets-valid topic
✅ Quarantined 150 malformed records with rejection reasons

Step 2: Delta Lakehouse
✅ Bronze: 2850 records loaded with metadata
✅ Silver: MERGE upsert completed (15 duplicates deduplicated)
✅ Gold: Aggregates computed (12 statuses, 5 severity levels)

Step 3: Quality Gate
✅ Data quality score: 95%
✅ Duplicate check: PASS
✅ Null values check: PASS
✅ Schema validation: PASS

Step 4: RAG Pipeline
✅ Embeddings: 284 document chunks vectorized
✅ BM25 index: 2850 documents indexed
✅ Sample query: "How do I get a refund?" → 5 results ranked

Step 5: Orchestration
✅ Airflow DAG: All tasks COMPLETED
✅ Lineage: 5 events recorded
✅ Status: SUCCESS
```

### ❌ Failure Case (Quality Gate)
```
Step 2: Delta Lakehouse
✅ Silver: MERGE upsert completed

Step 3: Quality Gate
❌ FAILED: Quality score 72% < threshold 80%
   - Duplicates: 450 (expected < 200)
   - Null values: 120 (expected < 50)

🛑 PIPELINE HALTED
   - Downstream tasks (RAG, metrics) NOT executed
   - Airflow raises AirflowException
   - Manual intervention required
```

---

##  Failure Paths Demonstrated

| Scenario | Evidence | Cell |
|---|---|---|
| **Malformed Record Rejection** | Missing ticket_id → routed to DLQ | 24 |
| **Schema Enforcement** | Wrong type write → Delta raises AnalysisException | 17 |
| **Quality Gate Halt** | 100 duplicates → quality score drops → pipeline stops | 34-36 |
| **Quarantine Zone** | Rejection reasons logged with timestamp | 34 |

---

##  How to Use This Project

1. **Learn:** Review the notebook cells in order (1-63)
2. **Understand:** Read inline documentation in src/ modules
3. **Run:** Execute all cells to see full pipeline
4. **Extend:** Modify src/ modules for your use cases
5. **Deploy:** Use src/dag_pipeline.py for production Airflow

---

## Training Program Attribution

**Program:** SDAIA Academy Data Engineering Track  
**Reference:** [SDAIA Academy on GitHub](https://github.com/SDAIAAcademy)

---

##  Troubleshooting

| Issue | Solution |
|---|---|
| Kafka connection fails | Windows detected; using file simulation  |
| pandas OverflowError | Restart kernel after install |
| Delta/PySpark not found | Check versions: pyspark 3.5.0, delta-spark 3.2.0 |
| Airflow tasks skip | Check AIRFLOW_HOME and db init |
| RAG embeddings slow | First run builds index; cached thereafter |

---

## 📝 License

MIT

