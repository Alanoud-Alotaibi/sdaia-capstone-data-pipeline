"""
Deliverable 3: RAG Pipeline.
Chunking + ChromaDB embeddings + BM25 index + hybrid search + reranking.
"""

import json
from typing import List, Tuple
from src.config import RAG_CHUNK_SIZE, RAG_CHUNK_OVERLAP, RAG_TOP_K, CHROMA_DIR, BM25_INDEX_PATH

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


def chunk_documents(df_gold, chunk_size: int = RAG_CHUNK_SIZE, overlap: int = RAG_CHUNK_OVERLAP) -> List[dict]:
    """
    Chunk documents (tickets) with overlap.
    Chunk size = ~500 chars, overlap = ~100 chars.
    """
    documents = []

    rows = df_gold.collect() if hasattr(df_gold, 'collect') else df_gold

    for i, row in enumerate(rows):
        description = str(row.Description or "")
        customer = str(row.Customer Name or "")
        status = str(row.Status or "")

        # Create chunks with overlap
        text = f"Ticket {row['Ticket ID']}: {customer} - {status}. {description}"

        for j in range(0, len(text), chunk_size - overlap):
            chunk = text[j : j + chunk_size]
            documents.append({
                "id": f"chunk_{i}_{j}",
                "text": chunk,
                "ticket_id": row.get("Ticket ID", ""),
                "source": "gold_layer",
            })

    return documents


def build_index(documents: List[dict]):
    """
    Build ChromaDB index + BM25 index.
    """
    if not HAS_CHROMA:
        print("⚠️ ChromaDB not available")
        return None

    try:
        client = chromadb.PersistentClient(path=CHROMA_DIR)
        collection = client.get_or_create_collection(name="tickets")

        # Batch insert (default: sentence-transformers embeddings)
        texts = [doc["text"] for doc in documents]
        ids = [doc["id"] for doc in documents]
        collection.add(ids=ids, documents=texts)

        print(f"✅ ChromaDB: {len(documents)} chunks indexed")
        return collection
    except Exception as e:
        print(f"❌ ChromaDB indexing failed: {e}")
        return None


def search_hybrid(query: str, collection, documents: List[dict], top_k: int = RAG_TOP_K) -> List[dict]:
    """
    Hybrid search: vector similarity + BM25.
    Combine results via Reciprocal Rank Fusion (RRF).
    """
    results = []

    if collection and HAS_CHROMA:
        # Vector search
        try:
            chroma_results = collection.query(query_texts=[query], n_results=top_k)
            for i, doc_id in enumerate(chroma_results["ids"][0]):
                score = 1.0 / (i + 1)  # RRF score
                results.append({"id": doc_id, "score": score, "method": "vector"})
        except Exception as e:
            print(f"❌ Vector search failed: {e}")

    if HAS_BM25:
        # BM25 search
        try:
            tokenized = [doc["text"].split() for doc in documents]
            bm25 = BM25Okapi(tokenized)
            query_tokens = query.split()
            bm25_scores = bm25.get_scores(query_tokens)

            # Top-K by BM25 score
            top_indices = sorted(range(len(bm25_scores)), key=lambda i: bm25_scores[i], reverse=True)[:top_k]
            for i, idx in enumerate(top_indices):
                score = 1.0 / (i + 1)  # RRF score
                results.append({"id": documents[idx]["id"], "score": score, "method": "bm25"})
        except Exception as e:
            print(f"❌ BM25 search failed: {e}")

    # Deduplicate & aggregate scores
    aggregated = {}
    for r in results:
        if r["id"] not in aggregated:
            aggregated[r["id"]] = {"score": 0, "methods": []}
        aggregated[r["id"]]["score"] += r["score"]
        aggregated[r["id"]]["methods"].append(r["method"])

    # Sort by aggregated score
    final_results = sorted(
        [{"id": k, **v} for k, v in aggregated.items()],
        key=lambda x: x["score"],
        reverse=True
    )[:top_k]

    return final_results


def answer_question(query: str, documents: List[dict], collection=None) -> str:
    """
    Answer a user question using RAG.
    Retrieve top documents, cite sources.
    """
    results = search_hybrid(query, collection, documents)

    if not results:
        return "No relevant documents found."

    # Get top document
    top_doc = next((d for d in documents if d["id"] == results[0]["id"]), None)

    if not top_doc:
        return "Error: document not found."

    answer = f"""
Answer to: "{query}"

Relevant information from ticket {top_doc['ticket_id']}:
{top_doc['text'][:200]}...

Source: {top_doc['source']} (match score: {results[0]['score']:.2f})
Methods used: {', '.join(results[0]['methods'])}
"""
    return answer
