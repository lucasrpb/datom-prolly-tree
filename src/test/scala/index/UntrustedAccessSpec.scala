import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable
import scala.util.Random

class UntrustedAccessSpec extends AnyFunSuite {

  private val chunker = new DatomFastCDC(ChunkerKey(1L, 2L), minKeys = 35, avgBytes = 2048, maxBytes = 16384, targetBranchingFactor = 64, maxKeys = 256)
  private val limits = WriteLimits(datomsPerSecond = 100.0, burst = 1000.0, churnWindowMs = 60000L, churnCost = 50.0)

  private class FakeClock(var nowMs: Long = 0L) { def apply(): Long = nowMs }

  private def follows(from: Int, until: Int): Vector[Datom] =
    Vector.range(from, until).map(i => Datom(i.toLong, 13L, (i + 1).toLong, 1L, op = true))

  private def newDatabase(clock: FakeClock) =
    new DatomDatabase(new DatabaseState(new DatomProllyTreeManager(new KVStore(), chunker)), new WriteRateLimiter(limits, () => clock()))

  test("writes are allowed up to the burst, then rate-limited until tokens refill") {
    val clock = new FakeClock()
    val limiter = new WriteRateLimiter(limits, () => clock())

    assert(limiter.acquire("alice", follows(0, 1000), Nil) == 0L)
    assert(limiter.acquire("alice", follows(1000, 1100), Nil) == 1000L) // 100 datoms at 100/s
    assert(limiter.acquire("bob", follows(2000, 2500), Nil) == 0L) // buckets are per client

    clock.nowMs = 1000L
    assert(limiter.acquire("alice", follows(1000, 1100), Nil) == 0L)
    assert(limiter.acquire("alice", follows(0, 1001), Nil) == Long.MaxValue) // can never fit
  }

  test("deleting recently inserted datoms is charged as churn, across clients, until the window passes") {
    val clock = new FakeClock()
    val limiter = new WriteRateLimiter(limits, () => clock())
    val probes = follows(0, 10)
    assert(limiter.acquire("alice", probes, Nil) == 0L)

    // 10 churn deletes cost 500 tokens, whoever sends them.
    assert(limiter.acquire("mallory", Nil, probes) == 0L)
    assert(limiter.acquire("mallory", Nil, probes) == 0L)
    assert(limiter.acquire("mallory", Nil, probes) > 0L)

    // Once the insert is older than the churn window, deleting it costs 1 per datom.
    clock.nowMs = limits.churnWindowMs
    assert(limiter.acquire("carol", Nil, probes) == 0L)
    assert(limiter.acquire("carol", Nil, follows(10, 1000)) == 0L) // 10 + 990 = 1000 tokens
  }

  test("a probing loop is throttled to fewer than 200 probes per minute") {
    val clock = new FakeClock()
    val limiter = new WriteRateLimiter(limits, () => clock())
    var probed = 0
    var next = 0
    // Insert a probe and delete it right away, as fast as the limiter allows, for one minute.
    while (clock.nowMs < 60000L) {
      val probe = follows(next, next + 1)
      val waitInsert = limiter.acquire("mallory", probe, Nil)
      if (waitInsert > 0) clock.nowMs += waitInsert
      else {
        var waitDelete = limiter.acquire("mallory", Nil, probe)
        while (waitDelete > 0) {
          clock.nowMs += waitDelete
          waitDelete = limiter.acquire("mallory", Nil, probe)
        }
        probed += 1
        next += 1
      }
    }
    // Without the churn charge it would be 3,500 (1,000 burst + 100/s * 60 s, 2 tokens each).
    assert(probed < 200, s"$probed probes in a minute")
  }

  test("the database commits valid batches and rejects oversized or rate-limited ones without changes") {
    val clock = new FakeClock()
    val db = newDatabase(clock)

    assert(db.transact("alice", follows(0, 800)) == TxResult.Committed)
    assert(db.count == 800)
    assert(db.entity(5L) == Seq(Datom(5L, 13L, 6L, 1L, op = true)))

    val tooBig = Datom(1L, 10L, "x" * chunker.datomSizeLimit, 2L, op = true)
    assert(db.transact("alice", Seq(tooBig)).isInstanceOf[TxResult.Rejected])
    assert(db.transact("alice", follows(800, 1100)).isInstanceOf[TxResult.RateLimited])
    assert(db.transact("alice", follows(0, 2000)).isInstanceOf[TxResult.Rejected])
    assert(db.count == 800)

    // Deleting the first 100 now would be churn (100 * 50 tokens); after the window it is not.
    assert(db.transact("alice", Nil, deletes = follows(0, 100)).isInstanceOf[TxResult.Rejected])
    clock.nowMs = limits.churnWindowMs
    assert(db.transact("alice", follows(800, 1100), deletes = follows(0, 100)) == TxResult.Committed)
    assert(db.datoms == follows(100, 1100))
    assert(db.entity(5L).isEmpty)
  }

  test("the database API exposes no hashes, nodes, sizes or store") {
    val leaky = Set[Class[_]](classOf[String], classOf[ProllyNode], classOf[LeafNode], classOf[InternalNode],
      classOf[KVStore], classOf[DatomProllyTreeManager], classOf[DatomFastCDC], classOf[ChunkerKey], classOf[Int])
    val publicMethods = classOf[DatomDatabase].getMethods.filter(m => m.getDeclaringClass == classOf[DatomDatabase] && !m.getName.contains("$"))
    assert(publicMethods.map(_.getName).toSet == Set("transact", "entity", "datoms", "count"))
    publicMethods.foreach(m => assert(!leaky.contains(m.getReturnType), s"${m.getName} returns ${m.getReturnType}"))
  }

  test("a client in node debt is blocked until its node budget refills") {
    val clock = new FakeClock()
    val limiter = new WriteRateLimiter(limits.copy(nodeWritesPerSecond = 100.0, nodeBurst = 500.0), () => clock())

    assert(limiter.acquire("mallory", follows(0, 1), Nil) == 0L)
    limiter.chargeNodes("mallory", 700) // one batch may overdraw: 500 - 700 = -200
    assert(limiter.acquire("mallory", follows(1, 2), Nil) == 2000L) // 200 nodes at 100/s
    assert(limiter.acquire("alice", follows(2, 3), Nil) == 0L) // other clients are unaffected

    clock.nowMs = 2000L
    assert(limiter.acquire("mallory", follows(1, 2), Nil) == 0L)
  }

  // Someone holding the chunker key builds a region with no content cuts, so every split
  // there is forced by maxKeys and each insert at the front re-splits the whole region.
  private def crafted(cdc: DatomFastCDC, n: Int, entity: Int => Long): Vector[Datom] = {
    val rnd = new Random(8)
    Vector.tabulate(n) { i =>
      Iterator.continually(Datom(entity(i), 10L, s"n${rnd.nextInt()}", 3L, op = true))
        .find(d => !cdc.cutsLeaf(d, chunkBytes = cdc.avgBytes)).get
    }
  }

  test("a leaked-key amplification attack is throttled, alerts, and stops after key rotation") {
    val clock = new FakeClock()
    val attackLimits = WriteLimits(datomsPerSecond = 100000.0, burst = 100000.0, nodeWritesPerSecond = 500.0, nodeBurst = 2000.0)
    val state = new DatabaseState(new DatomProllyTreeManager(new KVStore(), chunker))
    val alerts = mutable.ArrayBuffer[String]()
    val db = new DatomDatabase(state, new WriteRateLimiter(attackLimits, () => clock()), alerts += _)
    val admin = new DatomAdmin(state)

    val rnd = new Random(1)
    val honest = Vector.fill(20000)(Datom(1L + rnd.nextInt(5000), 13L, rnd.nextInt(5000).toLong, 1L, op = true)).distinct
    assert(db.transact("loader", honest) == TxResult.Committed)
    val base = 5000000L
    val region = crafted(chunker, 30000, i => base + 1 + i)
    assert(db.transact("mallory", region) == TxResult.Committed)
    clock.nowMs = 60000L // let the budgets refill

    // For one minute, insert single datoms at the front of the region as fast as allowed.
    // They are crafted too: a random one would sooner or later content-cut right before
    // the region, pinning its start and ending the amplification.
    val probes = crafted(chunker, 5000, _ => base).iterator
    val start = clock.nowMs
    val nodesBefore = state.manager.nodesWritten
    var committed = 0
    var next = probes.next()
    while (clock.nowMs < start + 60000L) {
      db.transact("mallory", Seq(next)) match {
        case TxResult.Committed => committed += 1; next = probes.next()
        case TxResult.RateLimited(waitMs) => clock.nowMs += waitMs
        case other => fail(other.toString)
      }
    }
    val nodes = state.manager.nodesWritten - nodesBefore
    val perInsert = nodes.toDouble / committed
    info(f"$committed amplified inserts in 60 s, $perInsert%.1f nodes each, $nodes nodes in total")

    assert(perInsert > 100, s"expected amplification, got $perInsert nodes per insert")
    // Work is capped by the node budget: burst + one minute of refill + one overdrawing batch.
    assert(nodes <= attackLimits.nodeBurst + attackLimits.nodeWritesPerSecond * 60 + perInsert * 2, s"$nodes nodes")
    assert(committed < 300, s"$committed amplified inserts got through")
    assert(alerts.nonEmpty && alerts.head.contains("mallory"))

    // Rotating the key turns the crafted region into ordinary data.
    val count = db.count
    admin.rotateKey(ChunkerKey(77L, 78L))
    assert(db.count == count)
    clock.nowMs += 600000L
    val afterRotation = state.manager.nodesWritten
    assert(db.transact("mallory", Seq(Datom(base, 10L, "after", 3L, op = true))) == TxResult.Committed)
    assert(state.manager.nodesWritten - afterRotation < 10)
    info(s"after rotation the same insert wrote ${state.manager.nodesWritten - afterRotation} nodes")
  }

  test("an honest client writing scattered batches at the full datom rate never hits the node budget") {
    val clock = new FakeClock()
    val state = new DatabaseState(new DatomProllyTreeManager(new KVStore(), chunker))
    val alerts = mutable.ArrayBuffer[String]()
    val db = new DatomDatabase(state, new WriteRateLimiter(WriteLimits(), () => clock()), alerts += _)
    val rnd = new Random(4)
    def scattered(n: Int) = Vector.fill(n)(Datom(1L + rnd.nextInt(20000), 13L, rnd.nextInt(20000).toLong, 1L, op = true))

    assert(db.transact("loader", scattered(5000)) == TxResult.Committed)
    for (second <- 1 to 120) {
      clock.nowMs = second * 1000L
      assert(db.transact("alice", scattered(1000)) == TxResult.Committed, s"second $second")
    }
    assert(alerts.isEmpty)
  }
}
