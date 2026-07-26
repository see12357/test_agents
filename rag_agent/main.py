"""
RAG Agent Service.
Subscribes to q.tasks.parsed, retrieves relevant documentation from ChromaDB.
Falls back to local file keyword search if ChromaDB is unavailable.
Strictly PEP 8 compliant.
"""

import logging
import os
from typing import Optional
import chromadb
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from shared.config import load_config
from shared.models import TaskEvent
from shared.db import save_task, get_prompt

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("RagasAgent")

# Load Configuration
settings, yaml_config = load_config()

# Setup RabbitMQ broker
rabbitmq_url = os.getenv("RABBITMQ_URL", settings.rabbitmq_url)
broker = RabbitBroker(rabbitmq_url)
app = FastStream(broker)

# Setup Chroma client and embeddings
chroma_host = os.getenv("CHROMA_HOST", settings.chroma_host)
chroma_port = int(os.getenv("CHROMA_PORT", str(settings.chroma_port)))

logger.info(f"ChromaDB target: {chroma_host}:{chroma_port}")
logger.info(f"Initializing embedding model: {yaml_config.embedding_model}")

try:
    embeddings = HuggingFaceEmbeddings(
        model_name=yaml_config.embedding_model,
        encode_kwargs={"normalize_embeddings": True}
    )
    chroma_client = chromadb.HttpClient(host=chroma_host, port=chroma_port)
    db = Chroma(
        client=chroma_client,
        collection_name="db_manuals",
        embedding_function=embeddings
    )
    try:
        coll = chroma_client.get_collection("db_manuals")
        if coll.count() == 0:
            logger.info("ChromaDB collection 'db_manuals' is empty. Auto-running seed_db...")
            from seed_db import main as seed_main
            seed_main()
    except Exception as seed_err:
        logger.warning(f"Auto-seeding check note: {seed_err}")
except Exception as e:
    logger.error(f"Failed to initialize Chroma connection: {e}")
    db = None

# Initialize CrossEncoder Re-ranker
try:
    from sentence_transformers import CrossEncoder
    reranker = CrossEncoder(yaml_config.rag_reranker_model)
    logger.info(f"Initialized CrossEncoder reranker ({yaml_config.rag_reranker_model})")
except Exception as re_err:
    logger.warning(f"Could not initialize CrossEncoder reranker: {re_err}")
    reranker = None


def _match_fallback_filename(query_lower: str) -> Optional[str]:
    """Maps search query keywords to deterministic DBA manual filename."""
    if any(k in query_lower for k in ["bouncer", "pgbouncer", "пул", "pool"]):
        return "pgbouncer_setup.md"
    if any(k in query_lower for k in ["миграци", "схем", "concurrently", "индекс"]):
        return "schema_migration.md"
    if any(k in query_lower for k in ["бэкап", "резервн", "patroni", "dump"]):
        return "backup_restore.md"
    if any(k in query_lower for k in ["ssl", "tls", "сертификат"]):
        return "cert_update.md"
    if any(k in query_lower for k in ["аудит", "безопасн", "compliance"]):
        return "compliance.md"
    if any(k in query_lower for k in ["os", "пакет", "обновлени"]):
        return "os_update.md"
    return None


def _scan_local_documents(docs_dir: str, query_lower: str) -> str:
    """Scans all markdown files in docs_dir by query word relevance."""
    matched_manuals = []
    query_words = set(query_lower.split())

    for filename in os.listdir(docs_dir):
        if filename.endswith(".md"):
            filepath = os.path.join(docs_dir, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()

            score = sum(1 for word in query_words if word in content.lower() or word in filename.lower())
            if score > 0:
                matched_manuals.append((score, filename, content))

    if matched_manuals:
        matched_manuals.sort(key=lambda x: x[0], reverse=True)
        result_parts = []
        for i, (_, filename, content) in enumerate(matched_manuals[:2]):
            result_parts.append(f"--- Manual {i+1}: {filename} ---\n{content}")
        return "\n\n".join(result_parts)

    return "No relevant documentation found."


def local_rag_fallback(query: str, reason: str = "ChromaDB fallback") -> str:
    """
    Scans the local documents directory and performs keyword matching.
    """
    logger.warning(f"RAG: {reason}. Running local document keyword scan fallback...")
    docs_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "documents"
    )

    if not os.path.exists(docs_dir):
        return "RAG Fallback Error: Documents directory not found."

    query_lower = query.lower()
    target_filename = _match_fallback_filename(query_lower)

    if target_filename:
        filepath = os.path.join(docs_dir, target_filename)
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return f"--- Manual (Matched Guideline: {target_filename}) ---\n" + f.read()

    return _scan_local_documents(docs_dir, query_lower)


rag_step = yaml_config.get_pipeline_step("rag")
SUBSCRIBE_QUEUE = rag_step.subscribe_queue if rag_step else "q.tasks.parsed"
PUBLISH_QUEUE = rag_step.publish_queue if rag_step else "q.tasks.ready_for_execution"


def _retrieve_chromadb_context(search_query: str) -> str:
    """Retrieves and reranks document candidates from ChromaDB."""
    if db is None:
        return local_rag_fallback(search_query, reason="ChromaDB client uninitialized")

    initial_k = yaml_config.rag_initial_candidates
    top_k = yaml_config.rag_reranker_top_k

    candidates = db.similarity_search(search_query, k=initial_k)
    logger.info(f"ChromaDB initial vector search returned {len(candidates)} candidates (config limit: {initial_k})")
    
    if not candidates:
        logger.info("ChromaDB returned 0 matches, running local manual keyword fallback...")
        return local_rag_fallback(search_query, reason="ChromaDB returned 0 matches")

    if reranker is not None:
        pairs = [[search_query, doc.page_content] for doc in candidates]
        scores = reranker.predict(pairs)
        scored_candidates = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
        results = [doc for score, doc in scored_candidates[:top_k]]
        for score, doc in scored_candidates[:top_k]:
            logger.info(f"[RERANKER {yaml_config.rag_reranker_model}] Score: {float(score):.4f} | Source: {doc.metadata.get('source')}")
    else:
        results = candidates[:top_k]

    manuals = [
        f"--- Manual {i+1} (Source: {doc.metadata.get('source', 'unknown')}) ---\n{doc.page_content}"
        for i, doc in enumerate(results)
    ]
    return "\n\n".join(manuals)


@broker.subscriber(SUBSCRIBE_QUEUE)
@broker.publisher(PUBLISH_QUEUE)
async def handle_rag_enrichment(event: TaskEvent) -> TaskEvent:
    """
    Subscribes to parsed tasks and enriches them with vector-stored guidelines.
    Falls back to local file parsing if ChromaDB is down.
    """
    if event.status == "failed":
        logger.warning(
            f"Bypassing RAG for failed task [{event.task_id}]: {event.error_message}"
        )
        return event

    logger.info(f"Received parsed task [{event.task_id}] for RAG enrichment")
    active_prompt = get_prompt("rag", yaml_config.rag_prompt)
    if active_prompt:
        logger.info(f"Using RAG Active System Prompt: {active_prompt[:80]}...")

    if not event.parsed_data or not event.parsed_data.subtasks:
        event.status = "failed"
        event.error_message = "RAG Error: Task has no parsed subtasks to query"
        logger.error(event.error_message)
        save_task(event)
        return event

    query_parts = [event.raw_text]
    for subtask in event.parsed_data.subtasks:
        query_parts.append(subtask.action)
    search_query = " ".join(query_parts)
    logger.info(f"Constructed RAG search query: '{search_query}'")

    try:
        event.rag_context = _retrieve_chromadb_context(search_query)
        event.status = "enriched"
        logger.info(f"[RAG SUCCESS] Task [{event.task_id}] enriched with documentation!")
    except Exception as err:
        logger.warning(f"ChromaDB retrieval error: {err}. Running local fallback...")
        try:
            event.rag_context = local_rag_fallback(search_query)
            event.status = "enriched"
        except Exception as fallback_err:
            event.status = "failed"
            event.error_message = f"RAG Enrichment Error: {str(err)} (Fallback failed: {fallback_err})"
            logger.error(f"[RAG FAILED] Task [{event.task_id}] enrichment error: {err}")

    save_task(event)
    return event
