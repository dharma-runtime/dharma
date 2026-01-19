# Task: Search and Indexing

## Objective
Implement derived indexes for fast text, vector, and graph search.

## Requirements
- **Commands**:
  - `index status`: Show index stats.
  - `index build <text|vector|graph>`: Trigger background indexing.
  - `index drop`: Remove indexes.
  - `find "query"`: Full-text search (Lucene-like or simple inverted index).
  - `vfind "query"`: Vector search (requires embedding model, likely minimal local one or API).
  - `gfind`: Graph traversal (refs/deps).
- **Search Engine**:
  - Use `tantivy` or similar for text search.
  - Use `sqlite` or specialized crate for graph/relation storage if needed, or keeping it memory-mapped.

## Implementation Details
- Indexes are **derived**: they can be rebuilt from the object store.
- **Text**: Index assertion bodies and notes.
- **Vector**: Optional, might need a feature flag or separate plugin to avoid heavy ML dependencies in the core.
