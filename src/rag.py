"""
Deliverable 3: Production Hybrid RAG Pipeline.
Includes Document Chunking, ChromaDB Vector Indexing, BM25 Keyword Search,
Reciprocal Rank Fusion (RRF), Cross-Encoder Reranking, Grounded Answers, and Citations.
"""

from typing import List, Dict, Any

from src.config import RAG_CHUNK_SIZE, RAG_CHUNK_OVERLAP, RAG_TOP_K, CHROMA_DIR, CROSS_ENCODER_MODEL

try:
    import chromadb
    HAS_CHROMA = True
except ImportError:
    HAS_CHROMA = False

try:
    from rank_bm25 import BM25Okapi
    HAS_BM25 = True
except ImportError:
    HAS_BM25 = False

try:
    from sentence_transformers import CrossEncoder
    HAS_CROSS_ENCODER = True
except ImportError:
    HAS_CROSS_ENCODER = False


def chunk_documents(df_or_records, chunk_size: int = RAG_CHUNK_SIZE, overlap: int = RAG_CHUNK_OVERLAP, max_docs: int = 1000) -> List[Dict[str, Any]]:
    """
    Chunk support ticket documents with overlap. Limit sample size for high-performance indexing.
    """
    documents = []
    
    if hasattr(df_or_records, "toPandas"):
        records = df_or_records.toPandas().head(max_docs).to_dict("records")
    elif hasattr(df_or_records, "to_dict"):
        records = df_or_records.to_dict("records")[:max_docs]
    else:
        records = list(df_or_records)[:max_docs]

    for i, r in enumerate(records):
        t_id = str(r.get("Ticket_ID") or r.get("Ticket ID") or f"TKT-{i}")
        customer = str(r.get("Customer_Name") or r.get("Customer Name") or "Unknown")
        category = str(r.get("Issue_Category") or r.get("Issue Category") or "General Inquiry")
        subject = str(r.get("Ticket_Subject") or r.get("Ticket Subject") or "General Support")
        desc = str(r.get("Ticket_Description") or r.get("Ticket Description") or r.get("description") or "No description provided.")
        agent = str(r.get("Assigned_Agent") or r.get("Assigned Agent") or "Unassigned")
        score = str(r.get("Satisfaction_Score") or r.get("Satisfaction score") or "5")

        full_text = (
            f"Ticket ID: {t_id} | Customer: {customer} | Category: {category} | "
            f"Subject: {subject} | Agent: {agent} | Satisfaction: {score} | "
            f"Description: {desc}"
        )

        step = max(1, chunk_size - overlap)
        for j in range(0, len(full_text), step):
            chunk = full_text[j : j + chunk_size]
            documents.append({
                "id": f"doc_{t_id}_chunk_{j}",
                "ticket_id": t_id,
                "customer_name": customer,
                "issue_category": category,
                "assigned_agent": agent,
                "satisfaction_score": score,
                "text": chunk,
                "source": "Delta_Gold_Layer"
            })

    print(f"[RAG] Chunked {len(records)} tickets into {len(documents)} document chunks")
    return documents


def build_vector_and_bm25_indices(documents: List[Dict[str, Any]]):
    """
    Build ChromaDB persistent vector collection and BM25 index.
    """
    chroma_collection = None
    bm25_index = None

    if HAS_CHROMA:
        try:
            client = chromadb.PersistentClient(path=CHROMA_DIR)
            try:
                client.delete_collection("support_tickets")
            except Exception:
                pass

            chroma_collection = client.create_collection(name="support_tickets")

            ids = [doc["id"] for doc in documents]
            texts = [doc["text"] for doc in documents]
            metadatas = [
                {
                    "ticket_id": doc["ticket_id"],
                    "category": doc["issue_category"],
                    "agent": doc["assigned_agent"]
                }
                for doc in documents
            ]

            chroma_collection.add(ids=ids, documents=texts, metadatas=metadatas)
            print(f"[OK] ChromaDB Vector Index built with {len(ids)} documents")
        except Exception as e:
            print(f"[WARN] ChromaDB indexing warning: {e}")

    if HAS_BM25:
        try:
            corpus_tokens = [doc["text"].lower().split() for doc in documents]
            bm25_index = BM25Okapi(corpus_tokens)
            print(f"[OK] BM25 Keyword Index built with {len(corpus_tokens)} documents")
        except Exception as e:
            print(f"[WARN] BM25 indexing warning: {e}")

    return chroma_collection, bm25_index


def hybrid_search_rrf(
    query: str,
    documents: List[Dict[str, Any]],
    chroma_collection: Any,
    bm25_index: Any,
    top_k: int = RAG_TOP_K * 2,
    rrf_k: int = 60
) -> List[Dict[str, Any]]:
    """
    Perform Hybrid Retrieval (Dense Vector + Sparse BM25) fused via Reciprocal Rank Fusion (RRF).
    """
    doc_map = {doc["id"]: doc for doc in documents}
    dense_ranks = {}
    bm25_ranks = {}

    # Dense Vector Search
    if chroma_collection and HAS_CHROMA:
        try:
            results = chroma_collection.query(query_texts=[query], n_results=min(top_k, len(documents)))
            if results and results["ids"] and results["ids"][0]:
                for rank, doc_id in enumerate(results["ids"][0]):
                    dense_ranks[doc_id] = rank + 1
        except Exception as e:
            print(f"[WARN] Dense vector search warning: {e}")

    # Sparse BM25 Search
    if bm25_index and HAS_BM25:
        try:
            query_tokens = query.lower().split()
            scores = bm25_index.get_scores(query_tokens)
            top_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
            for rank, idx in enumerate(top_indices):
                doc_id = documents[idx]["id"]
                bm25_ranks[doc_id] = rank + 1
        except Exception as e:
            print(f"[WARN] BM25 search warning: {e}")

    # Calculate RRF Scores
    all_doc_ids = set(dense_ranks.keys()).union(set(bm25_ranks.keys()))
    
    # Fallback to lexical keyword scoring if vector/BM25 libraries not present
    if not all_doc_ids:
        query_words = set(query.lower().split())
        scored_docs = []
        for doc in documents:
            text_words = set(doc["text"].lower().split())
            overlap = len(query_words.intersection(text_words))
            scored_docs.append((overlap, doc))
        scored_docs.sort(key=lambda x: x[0], reverse=True)
        return [dict(d[1], rrf_score=d[0] + 0.1) for d in scored_docs[:top_k]]

    rrf_candidates = []
    for doc_id in all_doc_ids:
        score = 0.0
        if doc_id in dense_ranks:
            score += 1.0 / (rrf_k + dense_ranks[doc_id])
        if doc_id in bm25_ranks:
            score += 1.0 / (rrf_k + bm25_ranks[doc_id])

        if doc_id in doc_map:
            candidate = dict(doc_map[doc_id])
            candidate["rrf_score"] = score
            candidate["dense_rank"] = dense_ranks.get(doc_id, 999)
            candidate["bm25_rank"] = bm25_ranks.get(doc_id, 999)
            rrf_candidates.append(candidate)

    rrf_candidates.sort(key=lambda x: x["rrf_score"], reverse=True)
    return rrf_candidates[:top_k]


def cross_encoder_rerank(
    query: str,
    candidates: List[Dict[str, Any]],
    top_k: int = RAG_TOP_K
) -> List[Dict[str, Any]]:
    """
    Rerank RRF candidate documents using Cross-Encoder model.
    """
    if not candidates:
        return []

    if HAS_CROSS_ENCODER:
        try:
            reranker = CrossEncoder(CROSS_ENCODER_MODEL)
            pairs = [(query, cand["text"]) for cand in candidates]
            scores = reranker.predict(pairs)
            for idx, score in enumerate(scores):
                candidates[idx]["cross_encoder_score"] = float(score)
            candidates.sort(key=lambda x: x["cross_encoder_score"], reverse=True)
            print(f"[OK] Cross-Encoder reranked top {len(candidates)} candidates")
            return candidates[:top_k]
        except Exception as e:
            print(f"[WARN] Cross-Encoder reranking fallback triggered: {e}")

    # Fallback Reranking heuristic
    query_words = set(query.lower().split())
    for cand in candidates:
        text_words = set(cand["text"].lower().split())
        overlap = len(query_words.intersection(text_words))
        cand["cross_encoder_score"] = cand["rrf_score"] + (overlap * 0.1)

    candidates.sort(key=lambda x: x["cross_encoder_score"], reverse=True)
    return candidates[:top_k]


def answer_query_with_citations(
    query: str,
    documents: List[Dict[str, Any]],
    chroma_collection: Any,
    bm25_index: Any
) -> Dict[str, Any]:
    """
    End-to-End RAG Execution: Hybrid Search (RRF) -> Cross-Encoder Reranking -> Grounded Answers with Citations.
    """
    candidates = hybrid_search_rrf(query, documents, chroma_collection, bm25_index)
    top_docs = cross_encoder_rerank(query, candidates, top_k=RAG_TOP_K)

    if not top_docs:
        top_docs = documents[:RAG_TOP_K]

    citations = []
    for i, doc in enumerate(top_docs, 1):
        cit = {
            "citation_id": f"[{i}]",
            "ticket_id": doc["ticket_id"],
            "customer_name": doc["customer_name"],
            "issue_category": doc["issue_category"],
            "assigned_agent": doc["assigned_agent"],
            "relevance_score": round(doc.get("cross_encoder_score", doc.get("rrf_score", 0.5)), 4)
        }
        citations.append(cit)

    top_ticket = top_docs[0]
    grounded_answer = (
        f"Based on historical resolution data for query '{query}':\n\n"
        f"Relevant support cases were identified under Category '{top_ticket['issue_category']}' "
        f"handled by Agent {top_ticket['assigned_agent']}. Key context: \"{top_ticket['text'][:250]}...\"\n\n"
        f"Source Citations:\n" + "\n".join([f"  - {c['citation_id']} Ticket ID: {c['ticket_id']} | Category: {c['issue_category']} | Agent: {c['assigned_agent']} (Relevance Score: {c['relevance_score']})" for c in citations])
    )

    return {
        "query": query,
        "answer": grounded_answer,
        "top_documents": top_docs,
        "citations": citations
    }
