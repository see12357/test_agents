"""
Langfuse distributed tracing for multi-agent pipeline.
Provides singleton Langfuse client and LangChain CallbackHandler factories.
"""

import logging
import os
from typing import Optional

logger = logging.getLogger("Tracing")

_langfuse_client: Optional[object] = None


def get_langfuse_client() -> Optional[object]:
    """Returns initialized Langfuse SDK client (singleton)."""
    global _langfuse_client
    secret_key = os.getenv("LANGFUSE_SECRET_KEY", "").strip()
    public_key = os.getenv("LANGFUSE_PUBLIC_KEY", "").strip()

    if not secret_key or not public_key:
        return None

    if _langfuse_client is None:
        try:
            from langfuse import Langfuse
            host = os.getenv("LANGFUSE_BASE_URL") or os.getenv("LANGFUSE_HOST") or "http://langfuse-web:3000"
            _langfuse_client = Langfuse(
                public_key=public_key, secret_key=secret_key, host=host
            )
            logger.info("Langfuse client initialized.")
        except Exception as exc:
            logger.warning(f"Langfuse client init failed: {exc}")
            return None

    return _langfuse_client


def get_langfuse_handler() -> Optional[object]:
    """
    Returns a fresh LangChain CallbackHandler (v3.x — no constructor args).
    Returns None if LANGFUSE_* env vars are missing.
    """
    secret_key = os.getenv("LANGFUSE_SECRET_KEY", "").strip()
    public_key = os.getenv("LANGFUSE_PUBLIC_KEY", "").strip()

    if not secret_key or not public_key:
        return None

    try:
        from langfuse.langchain import CallbackHandler
        return CallbackHandler()
    except Exception as exc:
        logger.error(f"Langfuse handler init failed: {exc}")
        return None
