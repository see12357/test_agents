

import os
import chromadb
from langchain_community.embeddings import HuggingFaceEmbeddings
from shared.config import PlatformSettings, YAMLConfig


def get_chroma_client_and_embeddings(settings: PlatformSettings, yaml_config: YAMLConfig):
    """
    Unified initializer for ChromaDB HTTP Client and HuggingFace Embeddings.
    Prevents code duplication across chunker and rag_agent.
    """
    chroma_host = os.getenv("CHROMA_HOST", settings.chroma_host)
    chroma_port = int(os.getenv("CHROMA_PORT", str(settings.chroma_port)))
    client = chromadb.HttpClient(host=chroma_host, port=chroma_port)

    embedding_model = yaml_config.embedding_model
    embeddings = HuggingFaceEmbeddings(model_name=embedding_model)

    return client, embeddings
