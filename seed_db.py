"""
Seeding script to populate ChromaDB with DB Support Engineer manuals.
Uses RecursiveCharacterTextSplitter and intfloat/multilingual-e5-large embeddings.
Strictly PEP 8 compliant.
"""

import os
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
import chromadb
from shared.config import load_config


def main() -> None:
    """
    Main function to read documents, chunk them, compute embeddings, and seed ChromaDB.
    """
    print("Loading configurations...")
    settings, yaml_config = load_config()

    chroma_host = os.getenv("CHROMA_HOST", "localhost")
    chroma_port = int(os.getenv("CHROMA_PORT", str(settings.chroma_port)))

    print(f"Connecting to ChromaDB at {chroma_host}:{chroma_port}...")
    try:
        client = chromadb.HttpClient(host=chroma_host, port=chroma_port)
    except Exception as e:
        print(f"Failed to connect to ChromaDB: {e}")
        return

    print(f"Initializing embedding model: {yaml_config.embedding_model}")
    embeddings = HuggingFaceEmbeddings(
        model_name=yaml_config.embedding_model,
        encode_kwargs={"normalize_embeddings": True}
    )

    from chunker import load_and_chunk_documents

    docs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "documents")
    print(f"Loading and chunking documents (chunk_size={yaml_config.chunk_size}, chunk_overlap={yaml_config.chunk_overlap})...")
    documents = load_and_chunk_documents(docs_dir=docs_dir, chunk_size=yaml_config.chunk_size, chunk_overlap=yaml_config.chunk_overlap)
    
    if not documents:
        print("No documents found to seed.")
        return

    print(f"Indexing {len(documents)} text chunks in ChromaDB...")
    try:
        Chroma.from_documents(
            documents=documents,
            embedding=embeddings,
            collection_name="db_manuals",
            client=client
        )
        print("ChromaDB successfully seeded!")
    except Exception as e:
        print(f"Error seeding ChromaDB: {e}")


if __name__ == "__main__":
    main()
