import java.io.{File, PrintWriter}
import java.security.{MessageDigest, SecureRandom}
import java.nio.ByteBuffer
import scala.collection.mutable
import scala.io.Source

// ==========================================
// 1. DOMAIN & DATA STRUCTURES
// ==========================================

case class Datom(e: Long, a: Long, v: Any, t: Long, op: Boolean)

object Datom {
  def sizeInBytes(d: Datom): Int = {
    var size = 25
    d.v match {
      case s: String => size += s.length * 2
      case _: Long => size += 8
      case _ => size += 4
    }
    size
  }

  implicit val ordering: Ordering[Datom] = new Ordering[Datom] {
    override def compare(x: Datom, y: Datom): Int = {
      var cmp = java.lang.Long.compare(x.e, y.e)
      if (cmp != 0) return cmp
      cmp = java.lang.Long.compare(x.a, y.a)
      if (cmp != 0) return cmp

      cmp = (x.v, y.v) match {
        case (v1: Long, v2: Long) => java.lang.Long.compare(v1, v2)
        case (v1: String, v2: String) => v1.compareTo(v2)
        case (v1, v2) => v1.toString.compareTo(v2.toString)
      }
      if (cmp != 0) return cmp

      java.lang.Long.compare(x.t, y.t)
    }
  }
}

sealed trait ProllyNode {
  def hash: String
  def firstKey: Datom
  def level: Int
  def byteSize: Int
}

case class LeafNode(hash: String, datoms: Seq[Datom]) extends ProllyNode {
  def firstKey: Datom = datoms.head
  def level: Int = 0
  def byteSize: Int = hash.length + datoms.map(Datom.sizeInBytes).sum
}

case class InternalNode(hash: String, children: Seq[(Datom, String)], level: Int) extends ProllyNode {
  def firstKey: Datom = children.head._1
  def byteSize: Int = hash.length + children.map { case (d, h) => Datom.sizeInBytes(d) + h.length }.sum
}

// ==========================================
// 2. IN-MEMORY KV STORE
// ==========================================

class KVStore {
  private val blocks = mutable.Map[String, ProllyNode]()

  def put(node: ProllyNode): Unit = blocks.put(node.hash, node)
  def get(hash: String): ProllyNode = blocks(hash)
  def contains(hash: String): Boolean = blocks.contains(hash)

  def size: Int = blocks.size
  def getAllNodes: Iterable[ProllyNode] = blocks.values
}

// ==========================================
// 3. FAST-CDC CHUNKER
// ==========================================

// Secret key for chunk-boundary decisions. It must stay the same for the life of a tree
// (persist it with the tree) and must never be exposed: whoever knows it can craft data
// that forces worst-case node sizes.
final case class ChunkerKey(k0: Long, k1: Long) {
  override def toString: String = "ChunkerKey(<redacted>)"
}

object ChunkerKey {
  def random(): ChunkerKey = {
    val rng = new SecureRandom()
    ChunkerKey(rng.nextLong(), rng.nextLong())
  }
}

// SipHash-2-4: a keyed PRF, so without the key nobody can predict its output, even
// after seeing many input/output pairs.
object SipHash24 {
  private final class State(k0: Long, k1: Long) {
    var v0: Long = 0x736f6d6570736575L ^ k0
    var v1: Long = 0x646f72616e646f6dL ^ k1
    var v2: Long = 0x6c7967656e657261L ^ k0
    var v3: Long = 0x7465646279746573L ^ k1

    def round(): Unit = {
      v0 += v1; v1 = java.lang.Long.rotateLeft(v1, 13); v1 ^= v0; v0 = java.lang.Long.rotateLeft(v0, 32)
      v2 += v3; v3 = java.lang.Long.rotateLeft(v3, 16); v3 ^= v2
      v0 += v3; v3 = java.lang.Long.rotateLeft(v3, 21); v3 ^= v0
      v2 += v1; v1 = java.lang.Long.rotateLeft(v1, 17); v1 ^= v2; v2 = java.lang.Long.rotateLeft(v2, 32)
    }

    def compress(m: Long): Unit = {
      v3 ^= m; round(); round(); v0 ^= m
    }
  }

  def hash(key: ChunkerKey, data: Array[Byte]): Long = {
    val s = new State(key.k0, key.k1)
    val fullBlocks = data.length / 8
    var i = 0
    while (i < fullBlocks) {
      var m = 0L
      var b = 7
      while (b >= 0) { m = (m << 8) | (data(i * 8 + b) & 0xFFL); b -= 1 }
      s.compress(m)
      i += 1
    }
    var last = (data.length & 0xFFL) << 56
    var b = data.length - fullBlocks * 8 - 1
    while (b >= 0) { last |= (data(fullBlocks * 8 + b) & 0xFFL) << (8 * b); b -= 1 }
    s.compress(last)

    s.v2 ^= 0xFF
    s.round(); s.round(); s.round(); s.round()
    s.v0 ^ s.v1 ^ s.v2 ^ s.v3
  }
}

class DatomFastCDC(
                    val key: ChunkerKey,
                    val minKeys: Int = 32,
                    val avgBytes: Int = 16384,
                    val maxBytes: Int = 131072,
                    val targetBranchingFactor: Int = 512,
                    val maxKeys: Int = 4096,
                    val maxDatomBytes: Int = -1
                  ) {

  // maxKeys and maxBytes are hard limits for every node, leaf or internal: a node is
  // closed before the item that would take it past either one, whatever the PRF says.
  // Node bytes are the sum of item sizes: Datom.sizeInBytes for a leaf datom, the key's
  // size plus the child hash length for an internal pointer.
  require(minKeys >= 1 && minKeys <= maxKeys, s"minKeys ($minKeys) must be between 1 and maxKeys ($maxKeys)")

  // Datoms larger than this are rejected. Kept well below maxBytes so that almost every
  // leaf ends at a content-defined cut rather than at the size cap.
  val datomSizeLimit: Int = if (maxDatomBytes > 0) maxDatomBytes else avgBytes / 4
  require(datomSizeLimit <= maxBytes, s"maxDatomBytes ($datomSizeLimit) must not exceed maxBytes ($maxBytes)")

  def validate(datom: Datom): Unit = {
    val size = Datom.sizeInBytes(datom)
    if (size > datomSizeLimit)
      throw new IllegalArgumentException(
        s"Datom (e=${datom.e}, a=${datom.a}) is $size bytes; the limit is $datomSizeLimit bytes")
  }

  // Unambiguous byte encoding of a datom, fed to the PRF.
  private def encode(d: Datom): Array[Byte] = {
    val (tag, valueBytes) = d.v match {
      case l: Long => (1.toByte, ByteBuffer.allocate(8).putLong(l).array())
      case s: String => (2.toByte, s.getBytes("UTF-8"))
      case other => (3.toByte, other.toString.getBytes("UTF-8"))
    }
    ByteBuffer.allocate(26 + valueBytes.length)
      .putLong(d.e).putLong(d.a).putLong(d.t)
      .put((if (d.op) 1 else 0).toByte).put(tag).put(valueBytes)
      .array()
  }

  // Cut chances are compared against the top 32 bits of a keyed PRF of the item alone.
  // Leaves: chance per datom is bytes / avgBytes, halved below avgBytes and doubled
  // above it (FastCDC-style normalization). Internal nodes: 1 / targetBranchingFactor,
  // halved below the target and doubled above it.
  private def leafCutThreshold(datomBytes: Int, chunkBytes: Int): Long = {
    val scaled = (datomBytes.toLong << 32) / avgBytes
    if (chunkBytes < avgBytes) scaled >> 1 else scaled << 1
  }

  private def internalCutThreshold(keys: Int): Long = {
    val scaled = (1L << 32) / targetBranchingFactor
    if (keys < targetBranchingFactor) scaled >> 1 else scaled << 1
  }

  /** Whether `datom` ends a leaf once minKeys is reached. Needs the secret key; exposed for tests. */
  def cutsLeaf(datom: Datom, chunkBytes: Int): Boolean =
    (SipHash24.hash(key, encode(datom)) >>> 32) < leafCutThreshold(Datom.sizeInBytes(datom), chunkBytes)

  // Decides chunk boundaries one item at a time. The state (item and byte counts) is
  // reset after every cut, so a boundary depends only on the items since the previous one.
  // That makes chunking a pure function of the item sequence (history independence)
  // and lets an incremental update stop once it re-aligns with an old boundary.
  //
  // For each item, callers first ask `fits`: if the current chunk is non-empty and the
  // item would break maxKeys or maxBytes, the chunk ends before it (`reset`). Then
  // `offer` adds it and says whether the chunk ends after it: at a content-defined cut
  // (once minKeys is reached) or because a hard limit is now exactly reached.
  abstract class Cutter[T] {
    private var keys = 0
    private var bytes = 0

    protected def itemBytes(item: T): Int
    protected def contentCut(item: T, keys: Int, bytes: Int): Boolean

    def fits(item: T): Boolean = keys == 0 || (keys < maxKeys && bytes + itemBytes(item) <= maxBytes)

    def offer(item: T): Boolean = {
      keys += 1
      bytes += itemBytes(item)
      val cut = keys >= maxKeys || bytes >= maxBytes || (keys >= minKeys && contentCut(item, keys, bytes))
      if (cut) reset()
      cut
    }

    def reset(): Unit = { keys = 0; bytes = 0 }
  }

  def newLeafCutter(): Cutter[Datom] = new Cutter[Datom] {
    override protected def itemBytes(datom: Datom): Int = Datom.sizeInBytes(datom)
    override protected def contentCut(datom: Datom, keys: Int, bytes: Int): Boolean = cutsLeaf(datom, bytes)
  }

  def newInternalCutter(): Cutter[(Datom, String)] = new Cutter[(Datom, String)] {
    override protected def itemBytes(child: (Datom, String)): Int = Datom.sizeInBytes(child._1) + child._2.length
    override protected def contentCut(child: (Datom, String), keys: Int, bytes: Int): Boolean =
      (SipHash24.hash(key, child._2.getBytes("UTF-8")) >>> 32) < internalCutThreshold(keys)
  }

  private def chunk[T](items: Iterator[T], cutter: Cutter[T]): Iterator[Seq[T]] = new Iterator[Seq[T]] {
    private val pending = items.buffered

    override def hasNext: Boolean = pending.hasNext

    override def next(): Seq[T] = {
      val currentChunk = mutable.ArrayBuffer[T]()
      var makeCut = false
      while (pending.hasNext && !makeCut) {
        if (!cutter.fits(pending.head)) {
          cutter.reset()
          makeCut = true
        } else {
          val item = pending.next()
          currentChunk += item
          makeCut = cutter.offer(item)
        }
      }
      currentChunk.toSeq
    }
  }

  def chunkDatoms(datoms: Iterator[Datom]): Iterator[Seq[Datom]] = chunk(datoms, newLeafCutter())

  def computeInternalBoundaries(children: Iterator[(Datom, String)]): Iterator[Seq[(Datom, String)]] =
    chunk(children, newInternalCutter())
}

// ==========================================
// 4. TREE MANAGER
// ==========================================

class DatomProllyTreeManager(val store: KVStore, val chunker: DatomFastCDC) {

  private var written = 0L

  /** Total nodes this manager has written; the difference across a call is that call's write cost. */
  def nodesWritten: Long = written

  private def put(node: ProllyNode): Unit = {
    store.put(node)
    written += 1
  }

  def computeLeafHash(datoms: Seq[Datom]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    datoms.foreach { d =>
      val buffer = ByteBuffer.allocate(33)
      buffer.putLong(d.e).putLong(d.a).putLong(d.t)
      buffer.put((if (d.op) 1 else 0).toByte)
      digest.update(buffer.array())
      digest.update(d.v.toString.getBytes("UTF-8"))
    }
    digest.digest().map("%02x".format(_)).mkString
  }

  def computeInternalHash(children: Seq[(Datom, String)]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    children.foreach { case (_, hash) => digest.update(hash.getBytes("UTF-8")) }
    digest.digest().map("%02x".format(_)).mkString
  }

  // The empty tree is a single leaf with no datoms.
  def emptyTree(): ProllyNode = {
    val node = LeafNode(computeLeafHash(Nil), Nil)
    put(node)
    node
  }

  def buildInitialTree(datoms: Seq[Datom]): ProllyNode = {
    datoms.foreach(chunker.validate)
    if (datoms.isEmpty) return emptyTree()
    val sorted = datoms.distinct.sorted
    val leaves = chunker.chunkDatoms(sorted.iterator).map { chunk =>
      val node = LeafNode(computeLeafHash(chunk), chunk)
      put(node)
      node
    }.toSeq

    buildInternalLayers(leaves, 1)
  }

  @scala.annotation.tailrec
  private def buildInternalLayers(nodes: Seq[ProllyNode], currentLevel: Int): ProllyNode = {
    if (nodes.length == 1) return nodes.head

    val pointers = nodes.map(n => (n.firstKey, n.hash))
    val boundaries = chunker.computeInternalBoundaries(pointers.iterator).toSeq

    val parents = boundaries.map { slice =>
      val internal = InternalNode(computeInternalHash(slice), slice, currentLevel)
      put(internal)
      internal
    }

    buildInternalLayers(parents, currentLevel + 1)
  }

  def insertBatch(rootHash: String, newDatoms: Seq[Datom]): ProllyNode =
    applyBatch(rootHash, inserts = newDatoms, deletes = Nil)

  /** Removes datoms equal to the given ones; datoms not in the tree are ignored. */
  def deleteBatch(rootHash: String, datoms: Seq[Datom]): ProllyNode =
    applyBatch(rootHash, inserts = Nil, deletes = datoms)

  // Applies inserts and deletes level by level, bottom-up. At each level only nodes whose
  // content changed are re-chunked, and re-chunking keeps pulling in the following
  // siblings until a cut lands exactly on an old node boundary. The result is the same
  // tree buildInitialTree would produce for the resulting data, whatever the history.
  // A datom present in both lists ends up deleted.
  def applyBatch(rootHash: String, inserts: Seq[Datom], deletes: Seq[Datom]): ProllyNode = {
    inserts.foreach(chunker.validate)
    val root = store.get(rootHash)
    if (inserts.isEmpty && deletes.isEmpty) return root
    if (isEmpty(root)) return buildInitialTree(inserts.filterNot(deletes.toSet))

    val levels = nodesByLevel(root)

    // Level 0: route each datom to the leaf whose key range contains it.
    val leaves = levels.head
    val added = mutable.Map[Int, mutable.ArrayBuffer[Datom]]()
    val removed = mutable.Map[Int, mutable.HashSet[Datom]]()
    inserts.foreach(d => added.getOrElseUpdate(findNodeIndex(leaves, d), mutable.ArrayBuffer[Datom]()) += d)
    deletes.foreach(d => removed.getOrElseUpdate(findNodeIndex(leaves, d), mutable.HashSet[Datom]()) += d)

    val newContents = mutable.Map[Int, Seq[Datom]]()
    (added.keySet ++ removed.keySet).foreach { i =>
      val old = leaves(i).asInstanceOf[LeafNode].datoms
      val gone = removed.getOrElse(i, mutable.HashSet.empty[Datom])
      val updated = (old ++ added.getOrElse(i, Nil)).distinct.filterNot(gone).sorted
      if (updated != old) newContents(i) = updated
    }
    if (newContents.isEmpty) return root

    var replacements = rechunkLevel[Datom](
      leaves.length,
      i => newContents.getOrElse(i, leaves(i).asInstanceOf[LeafNode].datoms),
      i => newContents.contains(i),
      i => leaves(i),
      () => chunker.newLeafCutter(),
      chunk => LeafNode(computeLeafHash(chunk), chunk)
    )

    var level = 1
    while (level < levels.length) {
      val below = replacements.flatten.toIndexedSeq
      if (below.isEmpty) return emptyTree()
      if (below.length == 1) return below.head

      val oldBelow = levels(level - 1)
      val changed = oldBelow.indices.map { j =>
        !(replacements(j).length == 1 && (replacements(j).head eq oldBelow(j)))
      }

      val nodes = levels(level)
      val childStart = nodes.scanLeft(0)((acc, n) => acc + n.asInstanceOf[InternalNode].children.length)
      val nodeLevel = level

      replacements = rechunkLevel[(Datom, String)](
        nodes.length,
        i => (childStart(i) until childStart(i + 1)).flatMap(j => replacements(j).map(n => (n.firstKey, n.hash))),
        i => (childStart(i) until childStart(i + 1)).exists(changed),
        i => nodes(i),
        () => chunker.newInternalCutter(),
        slice => InternalNode(computeInternalHash(slice), slice, nodeLevel)
      )
      level += 1
    }

    val top = replacements.flatten.toIndexedSeq
    if (top.isEmpty) emptyTree()
    else if (top.length == 1) top.head
    else buildInternalLayers(top, levels.length)
  }

  private def isEmpty(node: ProllyNode): Boolean = node match {
    case leaf: LeafNode => leaf.datoms.isEmpty
    case _ => false
  }

  // Returns the nodes of each level in key order, index 0 being the leaves.
  private def nodesByLevel(root: ProllyNode): IndexedSeq[IndexedSeq[ProllyNode]] = {
    val levels = mutable.ArrayBuffer[IndexedSeq[ProllyNode]](IndexedSeq(root))
    while (levels.last.head.level > 0) {
      levels += levels.last.flatMap(n => n.asInstanceOf[InternalNode].children.map(c => store.get(c._2)))
    }
    levels.reverse.toIndexedSeq
  }

  // Re-chunks one level. `contents(i)` is the new item list of old node i. Clean nodes are
  // reused while the cutter sits on a boundary; from a dirty node on, items are fed through
  // a fresh cutter until it cuts at the end of an old node followed by a clean one.
  // Returns, per old node, the nodes replacing it: the output of a re-chunked run is
  // attributed to its first old node and the other old nodes in the run get none.
  private def rechunkLevel[T](
                               count: Int,
                               contents: Int => Seq[T],
                               dirty: Int => Boolean,
                               oldNode: Int => ProllyNode,
                               newCutter: () => chunker.Cutter[T],
                               makeNode: Seq[T] => ProllyNode
                             ): Array[Seq[ProllyNode]] = {
    val out = Array.fill[Seq[ProllyNode]](count)(Nil)
    var i = 0
    while (i < count) {
      if (!dirty(i)) {
        out(i) = Seq(oldNode(i))
        i += 1
      } else {
        val start = i
        val cutter = newCutter()
        val emitted = mutable.ArrayBuffer[ProllyNode]()
        val current = mutable.ArrayBuffer[T]()

        def emit(): Unit = {
          val node = makeNode(current.toSeq)
          put(node)
          emitted += node
          current.clear()
        }

        var aligned = false
        while (!aligned) {
          contents(i).foreach { item =>
            if (!cutter.fits(item)) {
              emit()
              cutter.reset()
            }
            current += item
            if (cutter.offer(item)) emit()
          }
          i += 1
          if (i == count) {
            if (current.nonEmpty) emit()
            aligned = true
          } else if (!dirty(i)) {
            // Aligned if the chunk just ended, or if it must end before the next clean
            // node's first item because a hard limit would be broken.
            if (current.nonEmpty && !cutter.fits(contents(i).head)) {
              emit()
              cutter.reset()
            }
            aligned = current.isEmpty
          }
        }
        out(start) = emitted.toSeq
      }
    }
    out
  }

  private def findNodeIndex(nodes: IndexedSeq[ProllyNode], datom: Datom): Int = {
    var low = 0
    var high = nodes.length - 1
    var result = 0

    while (low <= high) {
      val mid = (low + high) / 2
      if (Datom.ordering.compare(datom, nodes(mid).firstKey) >= 0) {
        result = mid
        low = mid + 1
      } else {
        high = mid - 1
      }
    }
    result
  }

  def getAllDatoms(rootHash: String): Seq[Datom] = {
    store.get(rootHash) match {
      case leaf: LeafNode => leaf.datoms
      case internal: InternalNode =>
        internal.children.flatMap { case (_, childHash) =>
          getAllDatoms(childHash)
        }
    }
  }

  def getEntityDatoms(rootHash: String, entity: Long): Seq[Datom] = {
    store.get(rootHash) match {
      case leaf: LeafNode => leaf.datoms.filter(_.e == entity)
      case internal: InternalNode =>
        val children = internal.children
        children.indices.flatMap { i =>
          // Child i holds keys from its first key up to the next child's first key.
          val mayContain = children(i)._1.e <= entity && (i + 1 == children.length || children(i + 1)._1.e >= entity)
          if (mayContain) getEntityDatoms(children(i)._2, entity) else Nil
        }
    }
  }

  def countDatoms(rootHash: String): Long = {
    store.get(rootHash) match {
      case leaf: LeafNode => leaf.datoms.length.toLong
      case internal: InternalNode =>
        internal.children.map { case (_, childHash) => countDatoms(childHash) }.sum
    }
  }

  def getTreeByteSize(rootHash: String): Long = {
    store.get(rootHash) match {
      case leaf: LeafNode => leaf.byteSize.toLong
      case internal: InternalNode =>
        internal.byteSize.toLong + internal.children.map { case (_, childHash) =>
          getTreeByteSize(childHash)
        }.sum
    }
  }
}

// ==========================================
// 5. UNTRUSTED ACCESS
// ==========================================

// Limits for writes coming from untrusted clients.
//  - Every client has a token bucket: `datomsPerSecond` refill, up to `burst` tokens.
//  - Inserting or deleting a datom costs 1 token.
//  - Deleting a datom that anyone inserted less than `churnWindowMs` ago costs
//    `churnCost` tokens. Insert-then-delete is how someone probes for chunk boundaries
//    without leaving the probes in the tree, so it is made expensive. It is tracked
//    across clients, so inserting from one client and deleting from another doesn't help.
//  - Every client also has a node budget: `nodeWritesPerSecond` refill, up to `nodeBurst`.
//    Each committed batch is charged the nodes it actually wrote, after the fact, and a
//    client in debt cannot write until the budget refills. Normally a datom rewrites about
//    one node, but someone who knows the chunker key can craft a region with no content
//    cuts, where every insert re-splits the whole region. Charging by nodes makes that
//    amplification come out of the attacker's budget: per client, the tree does no more
//    work than for an honest client writing at full rate.
//  - A batch that writes at least `amplificationAlertMinNodes` nodes and more than
//    `amplificationAlertRatio` nodes per datom raises an alert: a sign the key leaked.
final case class WriteLimits(
                              datomsPerSecond: Double = 1000.0,
                              burst: Double = 5000.0,
                              churnWindowMs: Long = 60000L,
                              churnCost: Double = 50.0,
                              nodeWritesPerSecond: Double = 2000.0,
                              nodeBurst: Double = 10000.0,
                              amplificationAlertRatio: Double = 20.0,
                              amplificationAlertMinNodes: Long = 100L
                            )

class WriteRateLimiter(val limits: WriteLimits, clock: () => Long = () => System.currentTimeMillis()) {
  private final class Bucket(var tokens: Double, var nodeTokens: Double, var lastRefillMs: Long)

  private val buckets = mutable.Map[String, Bucket]()
  // Datoms inserted within the churn window, oldest first.
  private val recentInserts = mutable.LinkedHashMap[Datom, Long]()

  /** Charges the batch to `client`. Returns 0 if allowed, else the ms to wait before retrying. */
  def acquire(client: String, inserts: Seq[Datom], deletes: Seq[Datom]): Long = synchronized {
    val now = clock()
    forgetOldInserts(now)

    val bucket = refilled(client, now)
    val cost = inserts.length + deletes.map(d => if (recentInserts.contains(d)) limits.churnCost else 1.0).sum
    if (cost > limits.burst) return Long.MaxValue // can never fit; the batch must be split

    val datomWait = if (cost > bucket.tokens) math.ceil((cost - bucket.tokens) * 1000.0 / limits.datomsPerSecond).toLong else 0L
    val nodeWait = if (bucket.nodeTokens < 0) math.ceil(-bucket.nodeTokens * 1000.0 / limits.nodeWritesPerSecond).toLong max 1L else 0L
    val waitMs = datomWait max nodeWait
    if (waitMs == 0L) {
      bucket.tokens -= cost
      inserts.foreach { d => recentInserts.remove(d); recentInserts.put(d, now) }
    }
    waitMs
  }

  /** Charges `client` for the nodes a committed batch wrote. The budget may go negative. */
  def chargeNodes(client: String, nodes: Long): Unit = synchronized {
    refilled(client, clock()).nodeTokens -= nodes
  }

  private def refilled(client: String, now: Long): Bucket = {
    val bucket = buckets.getOrElseUpdate(client, new Bucket(limits.burst, limits.nodeBurst, now))
    val elapsed = now - bucket.lastRefillMs
    bucket.tokens = math.min(limits.burst, bucket.tokens + elapsed * limits.datomsPerSecond / 1000.0)
    bucket.nodeTokens = math.min(limits.nodeBurst, bucket.nodeTokens + elapsed * limits.nodeWritesPerSecond / 1000.0)
    bucket.lastRefillMs = now
    bucket
  }

  private def forgetOldInserts(now: Long): Unit = {
    while (recentInserts.nonEmpty && now - recentInserts.head._2 >= limits.churnWindowMs)
      recentInserts.remove(recentInserts.head._1)
  }
}

sealed trait TxResult
object TxResult {
  case object Committed extends TxResult
  final case class RateLimited(retryAfterMs: Long) extends TxResult
  final case class Rejected(reason: String) extends TxResult
}

// The current tree, shared by DatomDatabase (for clients) and DatomAdmin (for operators).
// Never hand this to clients.
final class DatabaseState(initialManager: DatomProllyTreeManager) {
  var manager: DatomProllyTreeManager = initialManager
  var root: ProllyNode = initialManager.emptyTree()
}

// The API to hand to untrusted clients. It exposes datoms only: no root or node hashes,
// no nodes, node sizes, block counts or tree stats. Those reveal where chunks end,
// which is exactly what someone probing for boundaries needs to observe. Writes go
// through the rate limiter, and a rejected batch changes nothing.
class DatomDatabase(state: DatabaseState, limiter: WriteRateLimiter, alert: String => Unit = _ => ()) {

  def transact(client: String, inserts: Seq[Datom], deletes: Seq[Datom] = Nil): TxResult = state.synchronized {
    try inserts.foreach(state.manager.chunker.validate)
    catch { case e: IllegalArgumentException => return TxResult.Rejected(e.getMessage) }

    limiter.acquire(client, inserts, deletes) match {
      case 0L =>
        val before = state.manager.nodesWritten
        state.root = state.manager.applyBatch(state.root.hash, inserts, deletes)
        val nodes = state.manager.nodesWritten - before
        limiter.chargeNodes(client, nodes)

        val datoms = inserts.length + deletes.length
        val limits = limiter.limits
        if (nodes >= limits.amplificationAlertMinNodes && nodes > limits.amplificationAlertRatio * (datoms max 1))
          alert(s"Write amplification: client '$client' changed $datoms datoms and rewrote $nodes nodes. " +
            "Crafted data like this needs the chunker key; consider DatomAdmin.rotateKey.")
        TxResult.Committed
      case Long.MaxValue => TxResult.Rejected("Batch is larger than the write burst limit; split it")
      case waitMs => TxResult.RateLimited(waitMs)
    }
  }

  def entity(e: Long): Seq[Datom] = state.synchronized(state.manager.getEntityDatoms(state.root.hash, e))
  def datoms: Seq[Datom] = state.synchronized(state.manager.getAllDatoms(state.root.hash))
  def count: Long = state.synchronized(state.manager.countDatoms(state.root.hash))
}

// Operator-only controls. Not part of the client API.
class DatomAdmin(state: DatabaseState) {

  // Rebuilds the tree under a new chunker key (same settings) with a bulk load. Data
  // crafted against the old key becomes ordinary data, so any cut-avoiding region stops
  // amplifying writes. Costs one full rebuild. The new tree goes into a fresh store, so
  // past versions are dropped. Persist the new key with the tree.
  def rotateKey(newKey: ChunkerKey): Unit = state.synchronized {
    val old = state.manager
    val c = old.chunker
    val chunker = new DatomFastCDC(newKey, c.minKeys, c.avgBytes, c.maxBytes, c.targetBranchingFactor, c.maxKeys, c.maxDatomBytes)
    val manager = new DatomProllyTreeManager(new KVStore(), chunker)
    state.root = manager.buildInitialTree(old.getAllDatoms(state.root.hash))
    state.manager = manager
  }
}

// ==========================================
// 6. DEMO EXECUTION
// ==========================================

object ProllyTreeDemo extends App {

  // Attribute ids used in the `a` field of Datom.
  object Attributes {
    final val UserName: Long = 10L
    final val UserAge: Long = 11L
    final val UserEmail: Long = 12L
    final val Follows: Long = 13L
    final val LikesFruit: Long = 14L
    // Set on a follow-edge entity (see followEdgeId): epoch millis when the follow started.
    final val FollowStartedAt: Long = 15L

    // Attributes whose value is a String; every other attribute holds a Long.
    val stringValued: Set[Long] = Set(UserName, UserEmail, LikesFruit)

    val names: Map[Long, String] = Map(
      UserName -> "UserName", UserAge -> "UserAge", UserEmail -> "UserEmail",
      Follows -> "Follows", LikesFruit -> "LikesFruit", FollowStartedAt -> "FollowStartedAt"
    )
  }

  val Fruits: Seq[String] = Seq(
    "Apple", "Banana", "Cherry", "Grape", "Mango", "Orange", "Peach", "Pear",
    "Pineapple", "Strawberry", "Watermelon", "Kiwi", "Blueberry", "Papaya", "Lemon"
  )

  val CsvPath = "1M_datoms.csv"
  val TotalDatoms = 1000000
  val NumUsers = 10000
  val MinFruitsPerUser = 1
  val MaxFruitsPerUser = 5
  val MinBatchSize = 10
  val MaxBatchSize = 1000
  val UsersTx = 1L
  val FollowsStartMs = 1704067200000L // 2024-01-01T00:00:00Z

  // Entity id of the "follower follows followee" relationship, used to attach
  // edge attributes like FollowStartedAt. Offset keeps it clear of user ids.
  val FollowEdgeIdBase = 1000000000L
  def followEdgeId(follower: Long, followee: Long): Long =
    FollowEdgeIdBase + follower * (NumUsers + 1) + followee

  def timed[T](label: String)(block: => T): (T, Long) = {
    val start = System.nanoTime()
    val result = block
    val elapsedMs = (System.nanoTime() - start) / 1000000
    println(f"   [$label] took $elapsedMs%,d ms")
    (result, elapsedMs)
  }

  // Users are written first (tx 1), then follow relationships grouped by tx id:
  // each follow tx is one random-sized batch of 10..1000 datoms. Every follow is
  // two datoms: `follower Follows followee` and `edge FollowStartedAt millis`.
  def generateCsv(path: String): Unit = {
    val rnd = new scala.util.Random(42L)
    val out = new PrintWriter(new java.io.BufferedWriter(new java.io.FileWriter(path), 1 << 20))
    try {
      out.println("e,a,v,t,op")

      val userFruits = Array.fill(NumUsers) {
        rnd.shuffle(Fruits).take(MinFruitsPerUser + rnd.nextInt(MaxFruitsPerUser - MinFruitsPerUser + 1))
      }
      // Follows come in pairs of datoms, so the remaining budget must be even.
      if ((TotalDatoms - NumUsers * 3 - userFruits.map(_.length).sum) % 2 != 0) {
        val idx = userFruits.indexWhere(_.length < Fruits.length)
        userFruits(idx) = userFruits(idx) :+ Fruits.find(f => !userFruits(idx).contains(f)).get
      }

      for (u <- 1L to NumUsers.toLong) {
        out.println(s"$u,${Attributes.UserName},User_$u,$UsersTx,true")
        out.println(s"$u,${Attributes.UserAge},${18 + rnd.nextInt(63)},$UsersTx,true")
        out.println(s"$u,${Attributes.UserEmail},user$u@example.com,$UsersTx,true")
        userFruits((u - 1).toInt).foreach(f => out.println(s"$u,${Attributes.LikesFruit},$f,$UsersTx,true"))
      }

      val minFollows = MinBatchSize / 2
      val maxFollows = MaxBatchSize / 2
      val seenPairs = mutable.HashSet[Long]()
      var remaining = (TotalDatoms - NumUsers * 3 - userFruits.map(_.length).sum) / 2
      var tx = UsersTx + 1
      var clockMs = FollowsStartMs
      while (remaining > 0) {
        val batchFollows =
          if (remaining <= maxFollows) remaining
          else {
            val s = minFollows + rnd.nextInt(maxFollows - minFollows + 1)
            if (remaining - s < minFollows) s - minFollows else s
          }
        // Each batch lands 1..60 s after the previous one; follows inside it within 1 s.
        clockMs += 1000L + rnd.nextInt(59000)
        var written = 0
        while (written < batchFollows) {
          val follower = 1L + rnd.nextInt(NumUsers)
          val followee = 1L + rnd.nextInt(NumUsers)
          if (follower != followee && seenPairs.add(followEdgeId(follower, followee))) {
            out.println(s"$follower,${Attributes.Follows},$followee,$tx,true")
            out.println(s"${followEdgeId(follower, followee)},${Attributes.FollowStartedAt},${clockMs + rnd.nextInt(1000)},$tx,true")
            written += 1
          }
        }
        remaining -= batchFollows
        tx += 1
      }
    } finally out.close()
  }

  def loadCsv(path: String): Vector[Datom] = {
    val src = Source.fromFile(path)
    try {
      src.getLines().drop(1).map { line =>
        val cols = line.split(',')
        val a = cols(1).toLong
        val v: Any = if (Attributes.stringValued.contains(a)) cols(2) else cols(2).toLong
        Datom(cols(0).toLong, a, v, cols(3).toLong, cols(4).toBoolean)
      }.toVector
    } finally src.close()
  }

  def percentile(sorted: IndexedSeq[Long], pct: Double): Long =
    sorted((sorted.length * pct).toInt min (sorted.length - 1))

  val store = new KVStore()
  val chunker = new DatomFastCDC(ChunkerKey.random(), minKeys = 35, avgBytes = 2048, maxBytes = 16384, targetBranchingFactor = 64, maxKeys = 256)
  val manager = new DatomProllyTreeManager(store, chunker)

  println(s"1. Preparing workload file $CsvPath...")
  if (new File(CsvPath).exists()) println("   File already exists, reusing it.")
  else timed("Generate CSV")(generateCsv(CsvPath))

  println("\n2. Loading datoms from CSV...")
  val (allDatoms, loadMs) = timed("Load CSV")(loadCsv(CsvPath))
  val userDatoms = allDatoms.filter(_.t == UsersTx)
  val followBatches = allDatoms.filter(_.t != UsersTx).groupBy(_.t).toVector.sortBy(_._1).map(_._2)
  println(f"   Loaded ${allDatoms.length}%,d datoms: ${userDatoms.length}%,d user datoms, " +
    f"${followBatches.map(_.length).sum}%,d follow datoms in ${followBatches.length}%,d batches")
  allDatoms.groupBy(_.a).toSeq.sortBy(_._1).foreach { case (a, ds) =>
    println(f"     ${Attributes.names.getOrElse(a, a.toString)}%-16s ${ds.length}%,9d datoms")
  }

  val followDatoms = followBatches.flatten.filter(_.a == Attributes.Follows)
  val selfFollows = followDatoms.count(d => d.e == d.v)
  val duplicateFollows = followDatoms.length - followDatoms.map(d => (d.e, d.v)).distinct.length
  if (selfFollows == 0 && duplicateFollows == 0)
    println("   Follow relationships OK: no self-follows, no user follows the same user twice.")
  else
    println(f"   ERROR: $selfFollows%,d self-follows and $duplicateFollows%,d duplicate follow pairs in the workload.")

  println(f"\n3. Inserting $NumUsers%,d users (${userDatoms.length}%,d datoms)...")
  val (beforeRoot, usersMs) = timed("Build users tree")(manager.buildInitialTree(userDatoms))
  val beforeStoreSize = store.size
  println(s"   Root after users:  ${beforeRoot.hash}")
  println(s"   Total KV Blocks:   $beforeStoreSize")
  println(f"   Tree Byte Size:    ${manager.getTreeByteSize(beforeRoot.hash) / (1024.0 * 1024.0)}%.2f MB")

  println(f"\n4. Inserting ${followBatches.length}%,d follow batches with insertBatch...")
  val batchTimesNs = new Array[Long](followBatches.length)
  val (afterRoot, followsMs) = timed("All follow batches") {
    var root = beforeRoot
    for ((batch, i) <- followBatches.zipWithIndex) {
      val start = System.nanoTime()
      root = manager.insertBatch(root.hash, batch)
      batchTimesNs(i) = System.nanoTime() - start
      if ((i + 1) % 250 == 0 || i + 1 == followBatches.length)
        println(f"   ... ${i + 1}%,d/${followBatches.length}%,d batches, store blocks: ${store.size}%,d")
    }
    root
  }
  val sortedBatchMs = batchTimesNs.map(_ / 1000000).sorted.toIndexedSeq
  val followCount = followBatches.map(_.length).sum
  println(s"   Root after follows: ${afterRoot.hash}")
  println(f"   Tree Elements:      ${manager.countDatoms(afterRoot.hash)}%,d datoms")
  println(s"   Tree Levels:        ${afterRoot.level + 1} (root at level ${afterRoot.level}, leaves at level 0)")
  println(s"   Total KV Blocks:    ${store.size} (${store.size - beforeStoreSize} minted by batches)")
  println(f"   Tree Byte Size:     ${manager.getTreeByteSize(afterRoot.hash) / (1024.0 * 1024.0)}%.2f MB")
  println(f"   Throughput:         ${followCount * 1000.0 / (followsMs max 1)}%,.0f datoms/s")
  println(f"   Batch latency (ms): avg ${batchTimesNs.sum / 1e6 / batchTimesNs.length}%.2f | " +
    f"p50 ${percentile(sortedBatchMs, 0.50)} | p90 ${percentile(sortedBatchMs, 0.90)} | " +
    f"p99 ${percentile(sortedBatchMs, 0.99)} | max ${sortedBatchMs.last}")

  println("\n--- VERIFYING CURRENT DATA INTEGRITY (AFTER ALL BATCHES) ---")
  val (afterOk, verifyAfterMs) = timed("Verify after root") {
    manager.getAllDatoms(afterRoot.hash) == allDatoms.sorted
  }
  if (afterOk) println(f"SUCCESS: The NEW tree (${afterRoot.hash}) contains all ${allDatoms.length}%,d datoms in perfect order.")
  else println("ERROR: The new tree data verification failed.")

  println("\n--- VERIFYING HISTORICAL INTEGRITY (AFTER USERS, BEFORE FOLLOWS) ---")
  val (beforeOk, verifyBeforeMs) = timed("Verify before root") {
    manager.getAllDatoms(beforeRoot.hash) == userDatoms.sorted
  }
  if (beforeOk) println(s"SUCCESS: The OLD tree (${beforeRoot.hash}) perfectly retained its original state! Immutability verified.")
  else println("ERROR: The historical tree data was mutated or corrupted.")

  println("\n--- VERIFYING HISTORY INDEPENDENCE (BULK LOAD vs. INCREMENTAL) ---")
  val (bulkRoot, bulkMs) = timed("Bulk load all datoms") {
    new DatomProllyTreeManager(new KVStore(), chunker).buildInitialTree(allDatoms)
  }
  if (bulkRoot.hash == afterRoot.hash)
    println(s"SUCCESS: Bulk-loading the same ${allDatoms.length} datoms gives the same root (${bulkRoot.hash}).")
  else
    println(s"ERROR: Bulk-load root ${bulkRoot.hash} differs from incremental root ${afterRoot.hash}.")

  println("\n5. Unfollowing: deleting 25% of follows in random batches of 10..1000 datoms...")
  val unfollowRnd = new scala.util.Random(7L)
  val edgeTimes = followBatches.flatten.filter(_.a == Attributes.FollowStartedAt).map(d => d.e -> d).toMap
  // Each unfollow removes both datoms of the relationship.
  val unfollowDatoms = unfollowRnd.shuffle(followDatoms).take(followDatoms.length / 4)
    .flatMap(d => Seq(d, edgeTimes(followEdgeId(d.e, d.v.asInstanceOf[Long]))))
  val unfollowBatches = {
    val batches = mutable.ArrayBuffer[Vector[Datom]]()
    var rest = unfollowDatoms
    while (rest.nonEmpty) {
      val size = 2 * ((MinBatchSize + unfollowRnd.nextInt(MaxBatchSize - MinBatchSize + 1)) / 2)
      batches += rest.take(size)
      rest = rest.drop(size)
    }
    batches.toVector
  }
  val (finalRoot, unfollowMs) = timed("All unfollow batches") {
    unfollowBatches.foldLeft(afterRoot)((root, batch) => manager.deleteBatch(root.hash, batch))
  }
  val remainingDatoms = {
    val deleted = unfollowDatoms.toSet
    allDatoms.filterNot(deleted)
  }
  println(f"   Deleted ${unfollowDatoms.length}%,d datoms in ${unfollowBatches.length}%,d batches " +
    f"(${unfollowDatoms.length * 1000.0 / (unfollowMs max 1)}%,.0f datoms/s)")
  println(f"   Tree Elements:      ${manager.countDatoms(finalRoot.hash)}%,d datoms")
  println(s"   Tree Levels:        ${finalRoot.level + 1}")

  println("\n--- VERIFYING DELETES ---")
  val (deletesOk, verifyDeletesMs) = timed("Verify deletes") {
    manager.getAllDatoms(finalRoot.hash) == remainingDatoms.sorted &&
      new DatomProllyTreeManager(new KVStore(), chunker).buildInitialTree(remainingDatoms).hash == finalRoot.hash &&
      manager.countDatoms(afterRoot.hash) == allDatoms.length
  }
  if (deletesOk)
    println(s"SUCCESS: The tree after unfollows (${finalRoot.hash}) holds exactly the remaining datoms, " +
      "matches a bulk load of them, and the pre-delete root is intact.")
  else
    println("ERROR: Delete verification failed.")

  printTreeStats(manager, finalRoot.hash)

  println("\n==========================================")
  println("              TIMING SUMMARY              ")
  println("==========================================")
  println(f"Load CSV:              $loadMs%,8d ms")
  println(f"Insert users:          $usersMs%,8d ms")
  println(f"Insert follow batches: $followsMs%,8d ms")
  println(f"Verify after root:     $verifyAfterMs%,8d ms")
  println(f"Verify before root:    $verifyBeforeMs%,8d ms")
  println(f"Bulk load (check):     $bulkMs%,8d ms")
  println(f"Delete unfollows:      $unfollowMs%,8d ms")
  println(f"Verify deletes:        $verifyDeletesMs%,8d ms")
  println()

  // --- STATISTICAL ANALYSIS METHOD ---
  def printTreeStats(manager: DatomProllyTreeManager, rootHash: String): Unit = {
    val nodes = mutable.ArrayBuffer[ProllyNode]()

    def traverse(hash: String): Unit = {
      val node = manager.store.get(hash)
      nodes += node
      node match {
        case i: InternalNode => i.children.foreach(c => traverse(c._2))
        case _ =>
      }
    }
    traverse(rootHash)

    val leaves = nodes.collect { case l: LeafNode => l }
    val internals = nodes.collect { case i: InternalNode => i }

    def analyzeSubset(name: String, subset: Seq[ProllyNode]): Unit = {
      if (subset.isEmpty) return
      val keys = subset.map {
        case l: LeafNode => l.datoms.length
        case i: InternalNode => i.children.length
      }.sorted

      val bytes = subset.map(_.byteSize).sorted

      def p(arr: Seq[Int], pct: Double): Int = arr((arr.length * pct).toInt min (arr.length - 1))
      def avg(arr: Seq[Int]): Double = arr.sum.toDouble / arr.length

      println(s"\n--- $name STATS (${subset.length} nodes) ---")
      println(f"Num of Keys -> Avg: ${avg(keys)}%6.1f | p50: ${p(keys, 0.50)}%4d | p70: ${p(keys, 0.70)}%4d | p90: ${p(keys, 0.90)}%4d | p99: ${p(keys, 0.99)}%4d | max: ${keys.last}%4d")
      println(f"Node size   -> Avg: ${avg(bytes)}%6.1f | p50: ${p(bytes, 0.50)}%4d | p70: ${p(bytes, 0.70)}%4d | p90: ${p(bytes, 0.90)}%4d | p99: ${p(bytes, 0.99)}%4d | max: ${bytes.last}%4d")
    }

    println("\n==========================================")
    println("          TREE STATISTICAL ANALYSIS       ")
    println("==========================================")
    analyzeSubset("LEAF NODES (Level 0)", leaves.toSeq)
    analyzeSubset("INTERNAL NODES (Level 1+)", internals.toSeq)
  }
}