import org.scalatest.funsuite.AnyFunSuite

class UntrustedAccessSpec extends AnyFunSuite {

  private val chunker = new DatomFastCDC(ChunkerKey(1L, 2L), minKeys = 35, avgBytes = 2048, maxBytes = 16384, targetBranchingFactor = 64, maxKeys = 256)
  private val limits = WriteLimits(datomsPerSecond = 100.0, burst = 1000.0, churnWindowMs = 60000L, churnCost = 50.0)

  private class FakeClock(var nowMs: Long = 0L) { def apply(): Long = nowMs }

  private def follows(from: Int, until: Int): Vector[Datom] =
    Vector.range(from, until).map(i => Datom(i.toLong, 13L, (i + 1).toLong, 1L, op = true))

  private def newDatabase(clock: FakeClock) =
    new DatomDatabase(new DatomProllyTreeManager(new KVStore(), chunker), new WriteRateLimiter(limits, () => clock()))

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
}
