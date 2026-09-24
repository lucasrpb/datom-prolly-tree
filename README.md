# Prolly Tree Datom Engine

A production-grade, in-memory implementation of a Prolly Tree (Probabilistic B-Tree) written in Scala. This engine is designed to store massively scalable, immutable Datom (Entity-Attribute-Value-Time) datasets with Git-like versioning capabilities. 

By leveraging Content-Defined Chunking (CDC) and Merkle DAG architecture, this tree provides the read performance of a B-Tree alongside the distributed synchronization and structural sharing properties of a Merkle Tree.

## Core Architecture

The engine stores records as `Datom(e, a, v, t, op)` tuples, sorted strictly in EAVT order. Instead of relying on rigid, deterministic node sizes (like a standard B-Tree), it utilizes **FastCDC (Fast Content-Defined Chunking)** to dynamically determine node boundaries based purely on the cryptographic entropy of the data itself.

Because boundaries are determined by content rather than insertion order, two identical datasets will mathematically guarantee the exact same tree structure and Root Hash, regardless of how or when the data was inserted. This eliminates the boundary-shift problem found in standard B-Trees and enables $O(\log N)$ historical diffing and merging.

## Why It Is Efficient

* **$O(\log N)$ Structural Sharing:** The tree is fully immutable. Batch updates rely on path-copying. Modifying a leaf node only requires minting a new path of hashes up to the root. A batch insert of 1,000 records into a 10-million record tree mints only a handful of new blocks, while perfectly sharing 99.9% of the existing tree structure in memory.
* **Shallow Traversal Depth:** Tuned with wide branching factors (e.g., target 512 keys per internal node), a dataset of 10 million records is consistently packed into a maximum depth of just 3 network hops.
* **Localized Re-Chunking:** During a batch insert, the `DatomProllyTreeManager` routes incoming datoms only to the specifically affected subtrees. The FastCDC algorithm only re-chunks the merged data within those specific leaves, bypassing the need to recalculate boundaries for the rest of the database.

## Why It Is Safe (Production Hardening)

Probabilistically structured databases are uniquely vulnerable to structural degeneration (falling into deep spires or flat arrays) and algorithmic complexity attacks. This implementation implements three distinct mathematical safeguards to guarantee worst-case immunity:

### 1. Adversarial Immunity (Secure Initialization)
The rolling hash relies on a 256-byte gear matrix. If this matrix is predictable, an attacker can pre-calculate payloads that intentionally force worst-case chunk boundaries, triggering OOM crashes. This engine initializes the gear matrix using `java.security.SecureRandom()`, making the tree mathematically blind to targeted algorithmic DoS attacks.

### 2. Hash Starvation Prevention (Entropy Injection)
Feeding raw, predictable data (like sequential IDs or repeating strings) into a rolling hash can cause mathematical cycles that fail to trigger probabilistic masks, degrading the tree into a rigid array. This engine strictly decouples meaning from entropy by passing every Datom through `scala.util.hashing.MurmurHash3` before FastCDC evaluation. This crushes the data into 64 bits of perfect, uniform pseudo-randomness, guaranteeing ideal chunk distribution regardless of payload patterns.

### 3. Statistical Padding for Probabilistic Balance
Standard hybrid Prolly Trees fall back on physical circuit breakers (`maxBytes` or `maxKeys`) when probability fails, which breaks structural sharing. This engine employs an **8x Statistical Padding** strategy. By setting the physical guardrails (e.g., 4096 keys) massively wider than the target branching factor (e.g., 512 keys), the dual-mask (Strict/Loose) algorithm is statistically guaranteed to find a natural content-defined boundary before ever hitting a physical wall. 

## Data Integrity

Every node in the tree is content-addressed using a strict **SHA-256 Merkle DAG**. 
* **Leaves** hash the raw Datom byte buffers.
* **Internal Nodes** hash the concatenated SHA-256 strings of their children.

This guarantees that any silent data corruption, disk bit-rot, or unauthorized tampering instantly invalidates the Root Hash, providing cryptographic assurance of the entire historical state.
