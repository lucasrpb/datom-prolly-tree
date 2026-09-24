# Prolly Tree Datom Engine

An in-memory Prolly Tree (probabilistic B-tree) in Scala that stores immutable Datoms (Entity-Attribute-Value-Time) with Git-like versioning. Every version of the tree is a root hash; old roots stay readable forever, and new versions share every unchanged node with the old ones.

Node boundaries come from content-defined chunking (CDC), and every node is addressed by its SHA-256 hash, so the tree is both a search tree and a Merkle DAG.

## Core Architecture

Records are `Datom(e, a, v, t, op)` tuples kept in EAVT order. Leaves hold datoms; internal nodes hold `(firstKey, childHash)` pointers. Instead of fixed node sizes, a keyed hash of each item decides where one node ends and the next begins:

* **Leaves** are cut on datom boundaries using `SipHash-2-4(key, datom)`. Each datom gets a cut chance proportional to its size (`datomBytes / avgBytes`), halved while the chunk is below `avgBytes` and doubled above it (FastCDC-style normalization).
* **Internal nodes** are cut using `SipHash-2-4(key, childHash)`, targeting `targetBranchingFactor` children.
* **Hard limits, for every node:** a node closes *before* the item that would take it past `maxKeys` items or `maxBytes` bytes, whatever the hash says, so no node ever exceeds either. Node bytes are the sum of its items: `Datom.sizeInBytes` per datom in a leaf, key size plus child hash length per pointer in an internal node. Content-defined cuts are only considered once a node has `minKeys` items.

### History independence

The chunker's state (item and byte counts) is **reset after every cut**, so a boundary depends only on the items since the previous boundary. Chunking is therefore a pure function of the sorted datom sequence: **the same set of datoms always produces the same tree and the same root hash, no matter how many batches, inserts or deletes produced it.** A bulk load and a million incremental updates converge on the identical structure.

This is what makes cheap diffing and syncing possible: two trees can be compared top-down, and any subtree with the same hash is known to be identical without looking inside.

### Incremental inserts and deletes

`DatomProllyTreeManager.applyBatch(root, inserts, deletes)` (with `insertBatch` and `deleteBatch` as shorthands) updates the tree level by level, bottom-up:

1. Each inserted or deleted datom is routed to the leaf whose key range contains it. Deleting a datom that is not in the tree is a no-op.
2. Only nodes whose content changed are re-chunked. Re-chunking starts at a known boundary (the start of the first changed node) with a fresh chunker and **keeps pulling in the following siblings until a cut lands exactly on an old node boundary**. From there on the old chunking is guaranteed to repeat, so every following node is reused as-is.
3. The replaced nodes feed the same process one level up. The tree grows a level when the top level no longer fits in one node, and shrinks when a level collapses to one node. Deleting everything yields the empty tree (a single empty leaf).

Deletes use the same re-synchronisation, so a leaf that shrinks is merged with its neighbours rather than left behind as a fragment.

The earlier implementation re-chunked each touched leaf in isolation. The tail of every split had no real boundary and was never merged with its neighbour, so tiny leaves piled up with every batch and the tree's shape depended on insert order. Re-synchronising with the old boundaries fixes both problems.

## Workload Benchmark

`ProllyTreeDemo` runs a social-graph workload of **1,000,000 datoms** read from `1M_datoms.csv` (columns `e,a,v,t,op`). The file is generated with a fixed seed the first time the demo runs, and reused if it already exists.

| Attribute (`Attributes.*`) | Id | Value | Datoms |
|---|---|---|---|
| `UserName` | 10 | String | 10,000 |
| `UserAge` | 11 | Long (18–80) | 10,000 |
| `UserEmail` | 12 | String | 10,000 |
| `Follows` | 13 | Long (followee id) | 470,040 |
| `LikesFruit` | 14 | String, 1–5 per user | 29,920 |
| `FollowStartedAt` | 15 | Long (epoch ms) | 470,040 |

* 10,000 users are bulk-loaded in transaction 1.
* Follows are unique (no self-follows, no user follows the same user twice) and arrive in **1,833 batches of 10–1,000 datoms**, each its own transaction, each applied with `insertBatch`.
* Each follow is two datoms: `follower Follows followee`, and `FollowStartedAt` on a follow-edge entity whose id is `followEdgeId(follower, followee)`.

* 25% of the follow relationships are then deleted (both datoms of each) in 492 random batches of 10–1,000 datoms with `deleteBatch`.

The demo verifies that:

* the final root contains exactly the 1M datoms, in order;
* the root saved after the users load (before any follows) still returns exactly the original user datoms;
* bulk-loading all 1M datoms in one go yields the **same root hash** as the 1,833 incremental batches;
* after the deletes, the tree holds exactly the remaining 764,980 datoms, matches a bulk load of them, and the pre-delete root is intact.

### Results

The table compares the two implementations with `minKeys = 10, avgBytes = 2048, maxBytes = 8192, targetBranchingFactor = 64`. The demo now uses `minKeys = 35, maxKeys = 256, maxBytes = 16384` (see below).

| | Before the fix | After the fix |
|---|---|---|
| Tree levels | 8 | **4** |
| Leaves | 173,910 | **14,069** |
| Datoms per leaf (median) | 2 | **69** |
| Final tree size | 60.3 MB | **34.2 MB** |
| Blocks minted by the batches | 2.28M | **1.19M** |
| All follow batches | 21.1 s | **22.1 s** (~43k datoms/s) |
| Batch latency p50 / p99 | 11 / 33 ms | **11 / 32 ms** |
| Same root as a bulk load | not guaranteed | **yes (verified)** |

Deleting 235,020 datoms in 492 batches takes 6.6 s (~36k datoms/s). After the deletes, leaves still average 70 datoms (~2.4 KB) and internal nodes 72 children: shrinking does not fragment the tree either.

The "after" numbers use the keyed SipHash chunker. Hashing each datom with SipHash costs more than the previous unkeyed hash, which took 17.2 s for the same batches.

**With `minKeys = 35`** (half the ~70 datoms of a typical leaf), the follow batches take 19.6 s (p50 9 ms, p99 24 ms) and the deletes 6.2 s. After the deletes, leaves average 82 datoms (~2.8 KB) and internal nodes 83 children, with 4 levels. The higher floor makes nodes a little larger on average, and it cuts the worst case: no leaf except the last can hold fewer than 35 datoms.

**With hard limits `maxKeys = 256`, `maxBytes = 16384`** on every node, the follow batches take 20.4 s (p50 10 ms, p99 27 ms) and the deletes 6.4 s, with 4 levels. The largest leaf has 256 datoms, and the largest internal node 168 children, at 16,370 bytes including its own 64-byte hash. A few of the 114 internal nodes (the p99 is already 16,360 bytes) end at the byte limit, because pointers are large (~97 bytes, mostly the hex child hash).

## Running

```bash
sbt -J-Xmx16g "runMain ProllyTreeDemo"   # workload benchmark
sbt test                                 # property test
```

`src/test/scala/index/ProllyTreeSpec.scala` covers:

* SipHash-2-4 against the reference test vector;
* 30 random histories mixing inserts and deletes (duplicates, keys before everything, deletes of whole ranges and of absent datoms): contents correct, root equal to a bulk load, every past root unchanged;
* deleting everything (empty tree) and re-inserting (same root as before);
* 40,000 deletes in small batches leave no leaf below `minKeys`;
* oversized datoms are rejected and leave the store untouched;
* an attacker without the key who probes with inserts and deletes and then reuses what it learned cannot shrink leaves;
* an attacker with a leaked key can force `minKeys`-sized leaves, but no smaller;
* no node ever exceeds `maxKeys` or `maxBytes`, including under tight limits where most cuts are forced (still equal to a bulk load), and when an attacker with the key crafts datoms that never trigger a content cut.

`src/test/scala/index/UntrustedAccessSpec.scala` covers the rate limiter (burst, refill, per-client buckets, churn charged across clients), a probing loop throttled to under 200 probes a minute, `DatomDatabase` commits and rejections, that its API exposes no hashes, nodes or store, node-budget debt, a leaked-key amplification attack (throttled to the node budget, alerted, and ended by key rotation), and that an honest client at full rate never hits the node budget.

## Hardening

To fragment the tree on purpose, an attacker has to predict which datoms end a node. The engine prevents that and bounds the damage if it fails:

* **Keyed cut decisions.** Cuts are decided by SipHash-2-4, a keyed PRF, under a secret 128-bit `ChunkerKey`. Without the key, seeing any number of datoms and where nodes ended says nothing about new datoms, so the only way to find a datom that ends a node is to insert it. The decision depends on the whole datom, so a known cutter tells nothing about similar datoms either. `ChunkerKey.toString` is redacted to keep the key out of logs.
* **Datom size limit.** Every inserted datom is validated before anything is written; one larger than `maxDatomBytes` (default `avgBytes / 4`, never above `maxBytes`) throws `IllegalArgumentException` and the batch is rejected. This keeps almost every leaf ending at a content-defined cut instead of the `maxBytes` cap.
* **Untrusted access through `DatomDatabase`.** Deletes let an attacker test datoms without leaving them in the tree, so untrusted clients get a facade instead of the manager:
  * **Datoms only.** It offers `transact`, `entity`, `datoms` and `count`. Root and node hashes, nodes, node sizes, block counts and tree stats are hidden, because those show where nodes end. A test checks by reflection that no public method returns them.
  * **Rate-limited writes.** `WriteRateLimiter` gives each client a token bucket (default 1,000 datoms/s, burst 5,000); each inserted or deleted datom costs 1.
  * **Churn charge.** Deleting a datom inserted less than `churnWindowMs` ago (default 60 s), by any client, costs `churnCost` tokens (default 50). An insert-then-delete probing loop drops from about 3,500 to under 200 probes a minute with the test limits.
  * **All or nothing.** A rejected or rate-limited batch changes nothing.
* **Leaked-key write amplification.** The hard limits cannot stop someone holding the key: they can craft a region with no content cuts, where every split is forced by `maxKeys` and so depends on position. One datom inserted at the front of such a region then re-splits all of it (measured: 200 blocks per 1-datom insert for a 50,000-datom region, against 3 normally). Three defences:
  * **Node budget.** Each client also has a budget of nodes written (default 2,000/s, burst 10,000). A committed batch is charged the nodes it actually wrote, and a client in debt cannot write until it refills. The amplification comes out of the attacker's own budget, so per client the tree never does more work than for an honest client writing at full rate. In the test, a minute of attack gets 264 inserts through at 121 nodes each (32,049 nodes, the budget), and an honest client writing 1,000 scattered datoms a second never hits it.
  * **Alert.** A batch writing at least 100 nodes and more than 20 per datom calls the `alert` hook given to `DatomDatabase`. Honest writes rewrite about one node per datom, so this means the key has most likely leaked.
  * **Key rotation.** `DatomAdmin.rotateKey(newKey)`, an operator-only handle kept apart from `DatomDatabase`, rebuilds the tree under a new key with a bulk load. The crafted region becomes ordinary data: in the test the same insert then writes 3 nodes instead of 121.
* **Hard size bounds.** `maxKeys` and `maxBytes` are never exceeded, even by an attacker holding the key who avoids every content cut; such nodes just fill up to a limit (tested). A content cut needs at least `minKeys` items, so a node smaller than that exists only as the last one on a level, or when its next item would break `maxBytes`. Even an attacker holding the key can shrink leaves only to `minKeys` datoms (with `minKeys = 35`, about 2.4x more leaves than the normal average of 82 datoms, down from about 7x with `minKeys = 10`).

## Data Integrity

Every node is content-addressed with SHA-256:

* **Leaves** hash each datom's `e`, `a`, `t`, `op` and `v`.
* **Internal nodes** hash the concatenated hashes of their children.

Any change to any datom changes every hash on its path to the root, so a root hash identifies an entire version of the database.

## Limitations

* **The key must live as long as the tree.** The demo uses `ChunkerKey.random()`, so every run gets different root hashes. A persisted tree must be reopened with the same key: a tree split under another key (or other chunker settings) no longer re-synchronises, and updates would rewrite most of it. Changing the key means rebuilding with a bulk load. Keep one key per deployment and never share it across tenants, since anyone holding it can craft worst-case data.
* **Memory only, no garbage collection.** `KVStore` is an in-memory map and keeps every node ever written. Most of the 1.19M blocks the benchmark mints are superseded leaf versions that stay only because every past root remains readable.
* **Insert walks the node index.** `insertBatch` lists the nodes of every level before re-chunking. That is a pointer walk with no hashing, but it is proportional to the tree's node count rather than to the batch.
* **Timing is still observable.** `DatomDatabase` hides hashes, nodes and block counts, but how long a write takes still hints at how many nodes it rewrote. Anyone who can time writes precisely could still learn something about boundaries. The keyed hash keeps that knowledge from carrying over to other datoms, and the churn charge makes collecting it slow.
* **Limits are per client.** An attacker with many client identities multiplies their budgets; a global node budget, or tying clients to accounts, would be needed on top. Key rotation also drops past versions (the new tree goes into a fresh store).
* **Limits live in memory.** `WriteRateLimiter` state resets on restart and is per process. With several writers, the limits would need a shared store.
* **Deletes are physical.** `deleteBatch` removes a datom from the index; it is separate from recording a retraction as a datom with `op = false`.
