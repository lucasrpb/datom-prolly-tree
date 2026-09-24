import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.util.Random

class ProllyTreeSpec extends AnyFunSuite {

  private val key = ChunkerKey(0x1234567890abcdefL, 0x0fedcba987654321L)
  private val chunker = new DatomFastCDC(key, minKeys = 35, avgBytes = 2048, maxBytes = 16384, targetBranchingFactor = 64, maxKeys = 256)

  private def newManager() = new DatomProllyTreeManager(new KVStore(), chunker)

  private def randomDatom(rnd: Random, maxEntity: Int): Datom =
    if (rnd.nextBoolean()) Datom(1L + rnd.nextInt(maxEntity), 10L, s"name_${rnd.nextInt(1000)}", 1L + rnd.nextInt(5), op = true)
    else Datom(1L + rnd.nextInt(maxEntity), 13L, rnd.nextInt(maxEntity).toLong, 1L + rnd.nextInt(5), op = true)

  private def leaves(manager: DatomProllyTreeManager, rootHash: String): Seq[LeafNode] =
    manager.store.get(rootHash) match {
      case leaf: LeafNode => Seq(leaf)
      case internal: InternalNode => internal.children.flatMap(c => leaves(manager, c._2))
    }

  private def bulkRoot(datoms: Seq[Datom]): String = newManager().buildInitialTree(datoms).hash

  private def allNodes(manager: DatomProllyTreeManager, rootHash: String): Seq[ProllyNode] =
    manager.store.get(rootHash) match {
      case leaf: LeafNode => Seq(leaf)
      case internal: InternalNode => internal +: internal.children.flatMap(c => allNodes(manager, c._2))
    }

  // Keys and bytes as the hard limits count them (item sizes, without the node's own hash).
  private def keysAndBytes(node: ProllyNode): (Int, Int) = node match {
    case l: LeafNode => (l.datoms.length, l.datoms.map(Datom.sizeInBytes).sum)
    case i: InternalNode => (i.children.length, i.children.map { case (k, h) => Datom.sizeInBytes(k) + h.length }.sum)
  }

  private def assertWithinLimits(manager: DatomProllyTreeManager, rootHash: String, cdc: DatomFastCDC, clue: String): Unit =
    allNodes(manager, rootHash).foreach { n =>
      val (keys, bytes) = keysAndBytes(n)
      assert(keys <= cdc.maxKeys && bytes <= cdc.maxBytes, s"$clue: node with $keys keys, $bytes bytes")
    }

  test("SipHash-2-4 matches the reference test vector") {
    // Key 00..0f, message 00..0e, from the SipHash paper.
    val refKey = ChunkerKey(0x0706050403020100L, 0x0f0e0d0c0b0a0908L)
    assert(SipHash24.hash(refKey, Array.tabulate[Byte](15)(_.toByte)) == 0xa129ca6149be45e5L)
  }

  test("random inserts and deletes produce the same tree as a bulk load of the result") {
    for (seed <- 1 to 30) {
      val rnd = new Random(seed)
      val manager = newManager()
      val maxEntity = 50 + rnd.nextInt(5000)

      var all = Vector.fill(1 + rnd.nextInt(3000))(randomDatom(rnd, maxEntity)).distinct
      var root = manager.buildInitialTree(all)
      val history = mutable.ArrayBuffer((root.hash, all.sorted))

      for (_ <- 1 to 1 + rnd.nextInt(40)) {
        // Inserts include duplicates and a key before everything; deletes include
        // existing datoms (sometimes whole ranges) and datoms that were never inserted.
        val inserts = Vector.fill(rnd.nextInt(2000))(randomDatom(rnd, maxEntity)) ++
          rnd.shuffle(all).take(rnd.nextInt(20)) :+ Datom(0L, 1L, 0L, 0L, op = true)
        val deletes =
          if (rnd.nextInt(4) == 0) {
            val sorted = all.sorted
            val from = rnd.nextInt(sorted.length max 1)
            sorted.slice(from, from + rnd.nextInt(1500))
          } else rnd.shuffle(all).take(rnd.nextInt(all.length / 2 + 1)) ++ Vector.fill(5)(randomDatom(rnd, maxEntity))

        root = manager.applyBatch(root.hash, inserts, deletes)
        val deleted = deletes.toSet
        all = (all ++ inserts).distinct.filterNot(deleted)
        history += ((root.hash, all.sorted))
      }

      val expected = all.sorted
      assert(manager.getAllDatoms(root.hash) == expected, s"seed $seed: contents")
      assert(root.hash == bulkRoot(expected), s"seed $seed: root differs from bulk load")
      assertWithinLimits(manager, root.hash, chunker, s"seed $seed")
      history.foreach { case (hash, datoms) =>
        assert(manager.getAllDatoms(hash) == datoms, s"seed $seed: an old root was mutated")
      }
    }
  }

  test("deleting everything gives the empty tree, and inserting again rebuilds it") {
    val rnd = new Random(7)
    val manager = newManager()
    val datoms = Vector.fill(5000)(randomDatom(rnd, 2000)).distinct
    val full = manager.buildInitialTree(datoms)

    val empty = manager.deleteBatch(full.hash, rnd.shuffle(datoms))
    assert(manager.getAllDatoms(empty.hash).isEmpty)
    assert(empty.hash == newManager().emptyTree().hash)

    val refilled = manager.insertBatch(empty.hash, datoms)
    assert(refilled.hash == full.hash)
    assert(manager.deleteBatch(empty.hash, datoms).hash == empty.hash)
  }

  test("deleting a leaf's datoms merges it with its neighbours instead of leaving fragments") {
    val rnd = new Random(11)
    val manager = newManager()
    val datoms = Vector.fill(50000)(randomDatom(rnd, 20000)).distinct
    var root = manager.buildInitialTree(datoms)
    var remaining = datoms

    // Delete in many small random batches, the pattern that used to leave tiny leaves.
    for (batch <- rnd.shuffle(datoms).take(40000).grouped(200)) {
      root = manager.deleteBatch(root.hash, batch)
      remaining = remaining.filterNot(batch.toSet)
    }

    assert(root.hash == bulkRoot(remaining))
    val sizes = leaves(manager, root.hash).map(_.datoms.length)
    assert(sizes.init.forall(_ >= chunker.minKeys), s"leaf below minKeys: ${sizes.min}")
  }

  test("datoms over the size limit are rejected and leave the tree untouched") {
    val manager = newManager()
    val root = manager.buildInitialTree(Vector.tabulate(1000)(i => Datom(i.toLong, 10L, s"user_$i", 1L, op = true)))
    val blocksBefore = manager.store.size
    val tooBig = Datom(5L, 10L, "x" * chunker.datomSizeLimit, 2L, op = true)
    assert(Datom.sizeInBytes(tooBig) > chunker.datomSizeLimit)

    val onInsert = intercept[IllegalArgumentException](manager.insertBatch(root.hash, Seq(Datom(1L, 10L, "ok", 2L, op = true), tooBig)))
    assert(onInsert.getMessage.contains("limit"))
    intercept[IllegalArgumentException](manager.applyBatch(root.hash, Seq(tooBig), Nil))
    intercept[IllegalArgumentException](newManager().buildInitialTree(Seq(tooBig)))
    assert(manager.store.size == blocksBefore)

    val atLimit = Datom(5L, 10L, "x" * ((chunker.datomSizeLimit - 25) / 2), 2L, op = true)
    assert(Datom.sizeInBytes(atLimit) <= chunker.datomSizeLimit)
    manager.insertBatch(root.hash, Seq(atLimit))
    intercept[IllegalArgumentException](new DatomFastCDC(key, maxBytes = 8192, maxDatomBytes = 8193))
  }

  // Honest data: users 1..5000. Attackers write into their own entity ranges so their
  // leaves can be measured on their own.
  private val honest = {
    val rnd = new Random(3)
    Vector.tabulate(5000)(u => Datom(u + 1L, 10L, s"User_${u + 1}", 1L, op = true)) ++
      Vector.fill(30000)(Datom(1L + rnd.nextInt(5000), 13L, 1L + rnd.nextInt(5000).toLong, 2L, op = true)).distinct
  }

  private def leavesIn(manager: DatomProllyTreeManager, rootHash: String, from: Long, until: Long): Seq[LeafNode] =
    leaves(manager, rootHash).filter(l => l.datoms.nonEmpty && l.datoms.head.e >= from && l.datoms.last.e < until)

  private def meanBytes(ls: Seq[LeafNode]): Double = ls.map(_.datoms.map(Datom.sizeInBytes).sum).sum.toDouble / ls.length

  test("an attacker without the key cannot learn to force small leaves, even with deletes") {
    val manager = newManager()
    var root = manager.buildInitialTree(honest)
    val baseline = meanBytes(leavesIn(manager, root.hash, 1, 5001))
    val rnd = new Random(99)

    // Probe: insert datoms, see which ones ended a leaf, delete the rest, repeat.
    val probeBase = 1000000L
    val knownCutters = mutable.ArrayBuffer[Datom]()
    for (round <- 0 until 20) {
      val probes = Vector.tabulate(2000)(i => Datom(probeBase + round * 2000 + i, 10L, s"p${rnd.nextInt()}", 3L, op = true))
      root = manager.insertBatch(root.hash, probes)
      val cutters = leavesIn(manager, root.hash, probeBase, probeBase + 1000000).map(_.datoms.last).toSet
      knownCutters ++= probes.filter(cutters)
      root = manager.deleteBatch(root.hash, probes)
    }
    assert(knownCutters.nonEmpty)

    // Exploit: pack an unused range with datoms built from what the probes revealed
    // (same values, neighbouring entities and times). With a keyed PRF, whether a
    // datom cuts depends on the whole datom, so none of this carries over.
    val attackBase = 3000000L
    val crafted = knownCutters.zipWithIndex.flatMap { case (d, i) =>
      Seq(d.copy(e = attackBase + i * 3), d.copy(e = attackBase + i * 3 + 1, t = d.t + 1), d.copy(e = attackBase + i * 3 + 2, a = 12L))
    }.toVector
    root = manager.insertBatch(root.hash, crafted)
    val attacked = leavesIn(manager, root.hash, attackBase, attackBase + 1000000)

    assert(attacked.nonEmpty)
    assert(meanBytes(attacked) >= baseline * 0.5,
      f"attacker shrank leaves to ${meanBytes(attacked)}%.0f bytes (baseline $baseline%.0f)")
  }

  test("an attacker with a leaked key is still bounded by minKeys") {
    val manager = newManager()
    var root = manager.buildInitialTree(honest)
    val rnd = new Random(5)

    // Worst case: the attacker has the key and crafts datoms that each end a leaf as
    // soon as minKeys allows.
    val attackBase = 2000000L
    val crafted = Vector.tabulate(5000) { i =>
      Iterator.continually(Datom(attackBase + i, 10L, s"a${rnd.nextInt()}", 3L, op = true))
        .find(d => chunker.cutsLeaf(d, chunkBytes = 0)).get
    }
    root = manager.insertBatch(root.hash, crafted)

    val attacked = leavesIn(manager, root.hash, attackBase, attackBase + 5000)
    // The attack range is at the end of the key space; the tree's last leaf is the one
    // leaf allowed below minKeys, since the data simply ends there.
    val sizes = attacked.map(_.datoms.length).init
    // The attack works, which is why the key must stay secret...
    assert(sizes.forall(_ == chunker.minKeys), s"sizes: ${sizes.distinct}")
    // ...but it cannot go below minKeys, so the damage is bounded.
    assert(attacked.length <= crafted.length / chunker.minKeys + 1)
    assert(root.hash == bulkRoot(honest ++ crafted))
  }

  test("with tight hard limits most cuts are forced, and the tree still matches a bulk load") {
    // Content cuts are rare with these settings, so maxKeys and maxBytes decide most boundaries.
    val tight = new DatomFastCDC(key, minKeys = 4, avgBytes = 2048, maxBytes = 700, targetBranchingFactor = 64, maxKeys = 16)
    for (seed <- 1 to 20) {
      val rnd = new Random(seed)
      val manager = new DatomProllyTreeManager(new KVStore(), tight)
      var all = Vector.fill(1 + rnd.nextInt(3000))(randomDatom(rnd, 3000)).distinct
      var root = manager.buildInitialTree(all)
      for (_ <- 1 to 1 + rnd.nextInt(30)) {
        val inserts = Vector.fill(rnd.nextInt(500))(randomDatom(rnd, 3000))
        val deletes = rnd.shuffle(all).take(rnd.nextInt(all.length / 3 + 1))
        root = manager.applyBatch(root.hash, inserts, deletes)
        all = (all ++ inserts).distinct.filterNot(deletes.toSet)
      }
      assert(manager.getAllDatoms(root.hash) == all.sorted, s"seed $seed: contents")
      assert(root.hash == new DatomProllyTreeManager(new KVStore(), tight).buildInitialTree(all).hash, s"seed $seed: bulk")
      assertWithinLimits(manager, root.hash, tight, s"seed $seed")
    }
  }

  test("an attacker with the key who avoids every content cut only fills nodes up to the hard limits") {
    val manager = newManager()
    var root = manager.buildInitialTree(honest)
    val rnd = new Random(8)

    // Datoms that never trigger a content cut, even with the looser above-average threshold.
    val attackBase = 4000000L
    val crafted = Vector.tabulate(20000) { i =>
      Iterator.continually(Datom(attackBase + i, 10L, s"n${rnd.nextInt()}", 3L, op = true))
        .find(d => !chunker.cutsLeaf(d, chunkBytes = chunker.avgBytes)).get
    }
    root = manager.insertBatch(root.hash, crafted)

    val attacked = leavesIn(manager, root.hash, attackBase, attackBase + 20000).init
    assert(attacked.nonEmpty)
    attacked.foreach { leaf =>
      val (keys, bytes) = keysAndBytes(leaf)
      assert(keys <= chunker.maxKeys && bytes <= chunker.maxBytes)
      // Every leaf ends because a hard limit was reached.
      assert(keys == chunker.maxKeys || bytes + 64 > chunker.maxBytes, s"leaf with $keys keys, $bytes bytes")
    }
    assertWithinLimits(manager, root.hash, chunker, "attacked tree")
    assert(root.hash == bulkRoot(honest ++ crafted))
  }
}
