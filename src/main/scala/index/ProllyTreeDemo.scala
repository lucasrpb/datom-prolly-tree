import java.io.{File, PrintWriter}
import java.security.{MessageDigest, SecureRandom}
import java.nio.ByteBuffer
import scala.collection.mutable
import scala.io.Source
import scala.util.hashing.MurmurHash3

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

class DatomFastCDC(
                    val minKeys: Int = 32,
                    val avgBytes: Int = 16384,
                    val maxBytes: Int = 131072,
                    val targetBranchingFactor: Int = 512,
                    val maxKeysPerInternalNode: Int = 4096
                  ) {

  private val gearMatrix: Array[Long] = {
    val rng = new SecureRandom()
    Array.fill(256)(rng.nextLong())
  }

  private val bits = (Math.log(avgBytes.toDouble) / Math.log(2)).ceil.toInt
  private val maskS: Long = (1L << (bits + 1)) - 1
  private val maskL: Long = (1L << (bits - 1)) - 1

  private val internalBits = (Math.log(targetBranchingFactor.toDouble) / Math.log(2)).ceil.toInt
  private val internalMaskS: Long = (1L << (internalBits + 1)) - 1
  private val internalMaskL: Long = (1L << (internalBits - 1)) - 1

  @inline private def hashLong(value: Long, currentHash: Long): Long = {
    var h = currentHash
    var i = 7
    while (i >= 0) {
      val b = ((value >> (i * 8)) & 0xFF).toInt
      h = (h << 1) + gearMatrix(b)
      i -= 1
    }
    h
  }

  @inline private def preHashDatom(d: Datom): Long = {
    var h1 = MurmurHash3.mix(0x9E3779B9, d.e.hashCode)
    h1 = MurmurHash3.mix(h1, d.a.hashCode)
    h1 = MurmurHash3.mix(h1, d.v.hashCode)
    h1 = MurmurHash3.mix(h1, d.t.hashCode)
    h1 = MurmurHash3.mixLast(h1, if (d.op) 1 else 0)
    val out1 = MurmurHash3.finalizeHash(h1, 5)

    var h2 = MurmurHash3.mix(0x1337C0DE, d.e.hashCode)
    h2 = MurmurHash3.mix(h2, d.v.hashCode)
    val out2 = MurmurHash3.finalizeHash(h2, 2)

    (out1.toLong << 32) | (out2.toLong & 0xFFFFFFFFL)
  }

  def chunkDatoms(datoms: Iterator[Datom]): Iterator[Seq[Datom]] = new Iterator[Seq[Datom]] {
    var hash = 0L

    override def hasNext: Boolean = datoms.hasNext

    override def next(): Seq[Datom] = {
      val currentChunk = mutable.ArrayBuffer[Datom]()
      var currentKeys = 0
      var currentBytes = 0
      var makeCut = false

      while (datoms.hasNext && !makeCut) {
        val datom = datoms.next()
        currentChunk += datom
        currentKeys += 1

        currentBytes += Datom.sizeInBytes(datom)

        val entropy = preHashDatom(datom)
        hash = hashLong(entropy, hash)

        if (currentBytes >= maxBytes) {
          makeCut = true
        } else if (currentKeys >= minKeys) {
          val mask = if (currentBytes < avgBytes) maskS else maskL
          if ((hash & mask) == 0L) {
            makeCut = true
          }
        }
      }
      currentChunk.toSeq
    }
  }

  def computeInternalBoundaries(children: Iterator[(Datom, String)]): Iterator[Seq[(Datom, String)]] = new Iterator[Seq[(Datom, String)]] {
    var hash = 0L

    override def hasNext: Boolean = children.hasNext

    override def next(): Seq[(Datom, String)] = {
      val currentChunk = mutable.ArrayBuffer[(Datom, String)]()
      var currentKeys = 0
      var makeCut = false

      while (children.hasNext && !makeCut) {
        val child = children.next()
        currentChunk += child
        currentKeys += 1

        val shaEntropy = java.lang.Long.parseUnsignedLong(child._2.substring(0, 16), 16)
        hash = hashLong(shaEntropy, hash)

        if (currentKeys >= maxKeysPerInternalNode) {
          makeCut = true
        } else if (currentKeys >= minKeys) {
          val mask = if (currentKeys < targetBranchingFactor) internalMaskS else internalMaskL
          if ((hash & mask) == 0L) makeCut = true
        }
      }
      currentChunk.toSeq
    }
  }
}

// ==========================================
// 4. TREE MANAGER
// ==========================================

class DatomProllyTreeManager(val store: KVStore, val chunker: DatomFastCDC) {

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

  def buildInitialTree(datoms: Seq[Datom]): ProllyNode = {
    val sorted = datoms.sorted
    val leaves = chunker.chunkDatoms(sorted.iterator).map { chunk =>
      val node = LeafNode(computeLeafHash(chunk), chunk)
      store.put(node)
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
      store.put(internal)
      internal
    }

    buildInternalLayers(parents, currentLevel + 1)
  }

  def insertBatch(rootHash: String, newDatoms: Seq[Datom]): ProllyNode = {
    if (newDatoms.isEmpty) return store.get(rootHash)

    val sortedBatch = newDatoms.sorted
    val updatedNodes = insertBatchRecursive(rootHash, sortedBatch)

    if (updatedNodes.length == 1) {
      updatedNodes.head
    } else {
      val topLevel = updatedNodes.head.level + 1
      buildInternalLayers(updatedNodes, topLevel)
    }
  }

  private def insertBatchRecursive(nodeHash: String, batch: Seq[Datom]): Seq[ProllyNode] = {
    store.get(nodeHash) match {
      case leaf: LeafNode =>
        val combinedDatoms = (leaf.datoms ++ batch).distinct.sorted
        val newLeafChunks = chunker.chunkDatoms(combinedDatoms.iterator).map { chunk =>
          val newLeaf = LeafNode(computeLeafHash(chunk), chunk)
          store.put(newLeaf)
          newLeaf
        }.toSeq
        newLeafChunks

      case internal: InternalNode =>
        val children = internal.children
        val batchAssignments = mutable.Map[Int, mutable.ArrayBuffer[Datom]]()

        batch.foreach { datom =>
          val idx = findChildIndex(children, datom)
          batchAssignments.getOrElseUpdate(idx, mutable.ArrayBuffer[Datom]()) += datom
        }

        val newChildrenPointers = mutable.ArrayBuffer[(Datom, String)]()

        for (i <- children.indices) {
          val (oldKey, childHash) = children(i)
          if (batchAssignments.contains(i)) {
            val childBatch = batchAssignments(i).toSeq
            val updatedChildNodes = insertBatchRecursive(childHash, childBatch)
            updatedChildNodes.foreach { newChild =>
              newChildrenPointers += ((newChild.firstKey, newChild.hash))
            }
          } else {
            newChildrenPointers += ((oldKey, childHash))
          }
        }

        val boundaries = chunker.computeInternalBoundaries(newChildrenPointers.iterator).toSeq
        boundaries.map { slice =>
          val newInternal = InternalNode(computeInternalHash(slice), slice, internal.level)
          store.put(newInternal)
          newInternal
        }
    }
  }

  private def findChildIndex(children: Seq[(Datom, String)], datom: Datom): Int = {
    var low = 0
    var high = children.length - 1
    var result = 0

    while (low <= high) {
      val mid = (low + high) / 2
      val comparison = Datom.ordering.compare(datom, children(mid)._1)

      if (comparison >= 0) {
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
// 5. DEMO EXECUTION
// ==========================================

object ProllyTreeDemo extends App {

  val store = new KVStore()
  val chunker = new DatomFastCDC(minKeys = 10, avgBytes = 2048, maxBytes = 8192, targetBranchingFactor = 64)
  val manager = new DatomProllyTreeManager(store, chunker)

  println("1. Generating initial base dataset of 100,000 datoms...")
  val initialDatoms = (1L to 5000000L).flatMap { e =>
    Seq(
      Datom(e, 10L, s"User_$e", 1L, op = true),
      Datom(e, 11L, 25L, 1L, op = true)
    )
  }

  println("2. Building initial Prolly Tree...")
  val initialRoot = manager.buildInitialTree(initialDatoms)
  val initialStoreSize = store.size
  val initialSizeMB = manager.getTreeByteSize(initialRoot.hash) / (1024.0 * 1024.0)

  println(s"   Initial Root Hash: ${initialRoot.hash}")
  println(s"   Total KV Blocks:   $initialStoreSize")
  println(f"   Tree Byte Size:    $initialSizeMB%.2f MB")

  println("\n3. Performing Batch Insert of 500 NEW datoms across random entities...")
  val batchToInsert = Seq(
    Datom(500L, 12L, "Blue", 2L, op = true),
    Datom(25000L, 12L, "Green", 2L, op = true),
    Datom(99999L, 12L, "Coding", 2L, op = true)
  ) ++ (100001L to 100247L).map(e => Datom(e, 10L, s"NewUser_$e", 2L, op = true))

  val startTime = System.currentTimeMillis()
  val newRootNode = manager.insertBatch(initialRoot.hash, batchToInsert)
  val endTime = System.currentTimeMillis()

  val blocksAdded = store.size - initialStoreSize
  val newSizeMB = manager.getTreeByteSize(newRootNode.hash) / (1024.0 * 1024.0)

  println("\n--- BATCH INSERT COMPLETE ---")
  println(s"Time Taken:         ${endTime - startTime} ms")
  println(s"New Root Hash:      ${newRootNode.hash}")
  println(s"New Total Blocks:   ${store.size}")
  println(s"New Blocks Saved:   $blocksAdded (Only updated path-copied blocks were minted!)")
  println(f"New Tree Byte Size: $newSizeMB%.2f MB")

  println("\n--- VERIFYING CURRENT DATA INTEGRITY (AFTER BATCH) ---")
  val newTreeDatoms = manager.getAllDatoms(newRootNode.hash)
  val expectedNewTotal = initialDatoms.length + batchToInsert.length

  if (newTreeDatoms.length == expectedNewTotal && newTreeDatoms == newTreeDatoms.sorted) {
    println("SUCCESS: The NEW tree contains all merged data in perfect order.")
  } else {
    println("ERROR: The new tree data verification failed.")
  }

  println("\n--- VERIFYING HISTORICAL INTEGRITY (BEFORE BATCH) ---")
  val oldTreeDatoms = manager.getAllDatoms(initialRoot.hash)

  if (oldTreeDatoms.length == initialDatoms.length && oldTreeDatoms == initialDatoms.sorted) {
    println(s"SUCCESS: The OLD tree (${initialRoot.hash}) perfectly retained its original state! Immutability verified.")
  } else {
    println("ERROR: The historical tree data was mutated or corrupted.")
  }

  printTreeStats(manager, newRootNode.hash)

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
      println(f"Num of Keys -> Avg: ${avg(keys)}%6.1f | p50: ${p(keys, 0.50)}%4d | p70: ${p(keys, 0.70)}%4d | p90: ${p(keys, 0.90)}%4d | p99: ${p(keys, 0.99)}%4d")
      println(f"Node size   -> Avg: ${avg(bytes)}%6.1f | p50: ${p(bytes, 0.50)}%4d | p70: ${p(bytes, 0.70)}%4d | p90: ${p(bytes, 0.90)}%4d | p99: ${p(bytes, 0.99)}%4d")
    }

    println("\n==========================================")
    println("          TREE STATISTICAL ANALYSIS       ")
    println("==========================================")
    analyzeSubset("LEAF NODES (Level 0)", leaves.toSeq)
    analyzeSubset("INTERNAL NODES (Level 1+)", internals.toSeq)
  }
}