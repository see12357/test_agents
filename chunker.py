"""
Document chunking and ChromaDB vector store seeding CLI.
"""

import os
import argparse
from typing import List, Optional
from langchain_community.vectorstores import Chroma
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from shared.config import load_config
from shared.chroma_util import get_chroma_client_and_embeddings


def load_and_chunk_documents(
    docs_dir: str = "documents",
    chunk_size: Optional[int] = 1000,
    chunk_overlap: Optional[int] = 200
) -> List[Document]:
    """Reads Markdown documents from docs_dir and splits them into chunks."""
    if not os.path.isabs(docs_dir):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        docs_dir = os.path.join(base_dir, docs_dir)

    if not os.path.exists(docs_dir):
        raise FileNotFoundError(f"Documents directory not found at: {docs_dir}")

    raw_documents = []
    for filename in sorted(os.listdir(docs_dir)):
        if filename.endswith(".md"):
            filepath = os.path.join(docs_dir, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()

            raw_documents.append(
                Document(
                    page_content=content,
                    metadata={
                        "source": filename,
                        "topic": filename.replace(".md", ""),
                        "file_path": filepath
                    }
                )
            )

    if not raw_documents:
        return []

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n## ", "\n### ", "\n\n", "\n", " "]
    )
    chunked_docs = splitter.split_documents(raw_documents)

    for i, doc in enumerate(chunked_docs):
        doc.metadata["chunk_id"] = i + 1
        doc.metadata["total_chunks"] = len(chunked_docs)

    return chunked_docs


def seed_database(preview_only: bool = False) -> None:
    """Chunks documents and indexes them in ChromaDB vector store."""
    settings, yaml_config = load_config()
    docs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "documents")

    chunk_size = yaml_config.chunk_size or 1000
    chunk_overlap = yaml_config.chunk_overlap or 200

    print(f"Loading and chunking documents (size={chunk_size}, overlap={chunk_overlap})...")
    documents = load_and_chunk_documents(docs_dir=docs_dir, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    
    if not documents:
        print("No documents found.")
        return

    print(f"Total chunks generated: {len(documents)}")
    if preview_only:
        for i, chunk in enumerate(documents[:3]):
            print(f"\n--- Chunk {i+1} [{chunk.metadata['source']}] ---")
            print(f"{chunk.page_content[:150]}...")
        return

    try:
        client, embeddings = get_chroma_client_and_embeddings(settings, yaml_config)
    except Exception as e:
        print(f"Failed to initialize ChromaDB embeddings client: {e}")
        return

    collection_name = yaml_config.chroma_collection_name
    print(f"Indexing {len(documents)} chunks in collection '{collection_name}' with model: {yaml_config.embedding_model}")

    try:
        Chroma.from_documents(
            documents=documents,
            embedding=embeddings,
            collection_name=collection_name,
            client=client
        )
        print(f"ChromaDB collection '{collection_name}' successfully seeded!")
    except Exception as e:
        print(f"Error seeding ChromaDB: {e}")


def main():
    parser = argparse.ArgumentParser(description="Document Chunker & ChromaDB Seeding CLI")
    parser.add_argument("--preview", action="store_true", help="Preview chunks without seeding ChromaDB")
    args = parser.parse_args()
    seed_database(preview_only=args.preview)


if __name__ == "__main__":
    main()
