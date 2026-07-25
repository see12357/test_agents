"""
Standalone Document Chunking Module for DB Maintenance Manuals.
Uses Markdown-aware RecursiveCharacterTextSplitter with rich metadata enrichment.
Strictly PEP 8 compliant.
"""

import os
import argparse
from typing import List, Optional
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter


from shared.config import load_config

_, _yaml_cfg = load_config()


def load_and_chunk_documents(
    docs_dir: str = "documents",
    chunk_size: Optional[int] = None,
    chunk_overlap: Optional[int] = None
) -> List[Document]:
    """
    Reads all Markdown documents from `docs_dir` and splits them into chunks.

    Args:
        docs_dir (str): Directory containing Markdown manuals.
        chunk_size (int): Maximum characters per chunk.
        chunk_overlap (int): Overlap character count between chunks.

    Returns:
        List[Document]: List of chunked Document objects.
    """
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

            topic = filename.replace(".md", "")
            raw_documents.append(
                Document(
                    page_content=content,
                    metadata={
                        "source": filename,
                        "topic": topic,
                        "file_path": filepath
                    }
                )
            )

    if not raw_documents:
        return []

    # Markdown-aware splitting
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n## ", "\n### ", "\n\n", "\n", " "]
    )
    chunked_docs = splitter.split_documents(raw_documents)

    # Attach chunk indexing metadata
    for i, doc in enumerate(chunked_docs):
        doc.metadata["chunk_id"] = i + 1
        doc.metadata["total_chunks"] = len(chunked_docs)

    return chunked_docs


def main():
    parser = argparse.ArgumentParser(description="Standalone Document Chunker CLI")
    parser.add_argument("--dir", default="documents", help="Path to documents folder")
    parser.add_argument("--size", type=int, default=1000, help="Chunk size")
    parser.add_argument("--overlap", type=int, default=200, help="Chunk overlap")
    args = parser.parse_args()

    chunks = load_and_chunk_documents(docs_dir=args.dir, chunk_size=args.size, chunk_overlap=args.overlap)
    print(f"=== Chunker Results ===")
    print(f"Source Directory: {args.dir}")
    print(f"Total Chunks Generated: {len(chunks)}")
    for i, chunk in enumerate(chunks[:3]):
        print(f"\n--- Chunk {i+1} [{chunk.metadata['source']}] ---")
        print(f"Content:\n{chunk.page_content[:150]}...")


if __name__ == "__main__":
    main()
