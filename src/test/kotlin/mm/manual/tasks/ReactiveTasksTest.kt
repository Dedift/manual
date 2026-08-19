package mm.manual.tasks

import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class ReactiveTasksTest {

  @Test
  fun `task 01 filters paid events and maps amounts to cents`() {
    val task = Task01MapFilter()

    val result = task.paidAmountsInCents(
      Flux.just(
        UserEvent("u1", "PAID", 12),
        UserEvent("u2", "DECLINED", 50),
        UserEvent("u3", "PAID", 0),
        UserEvent("u4", "PAID", 7),
      )
    )

    StepVerifier.create(result)
      .expectNext(1200, 700)
      .verifyComplete()
  }

  @Test
  fun `task 02 loads roles and flattens them`() {
    val task = Task02FlatMap()

    val result = task.userRoleLabels(Flux.just("ann", "bob")) { userId ->
      Mono.just(
        when (userId) {
          "ann" -> listOf("admin", "support")
          else -> listOf("viewer")
        }
      )
    }

    StepVerifier.create(result.sort())
      .expectNext("ann:admin", "ann:support", "bob:viewer")
      .verifyComplete()
  }

  @Test
  fun `task 03 saves records sequentially`() {
    val task = Task03ConcatMap()
    val started = mutableListOf<Int>()

    val result = task.saveSequentially(Flux.just(1, 2, 3)) { id ->
      started += id
      Mono.delay(Duration.ofMillis((4 - id) * 10L))
        .thenReturn(AuditRecord(id, "saved"))
    }

    StepVerifier.create(result)
      .expectNext(
        AuditRecord(1, "saved"),
        AuditRecord(2, "saved"),
        AuditRecord(3, "saved"),
      )
      .verifyComplete()

    kotlin.test.assertEquals(listOf(1, 2, 3), started)
  }

  @Test
  fun `task 04 combines account and balance`() {
    val task = Task04Zip()

    val result = task.accountView(
      accountId = "a-1",
      loadAccount = { Mono.just(Account(it, "Jane")) },
      loadBalance = { Mono.just(Balance(it, 9500)) },
    )

    StepVerifier.create(result)
      .expectNext(AccountView("a-1", "Jane", 9500))
      .verifyComplete()
  }

  @Test
  fun `task 05 falls back only for failed items`() {
    val task = Task05ErrorHandling()

    val result = task.loadNamesWithFallback(Flux.just(1, 2, 3)) { id ->
      if (id == 2) Mono.error(IllegalStateException("missing"))
      else Mono.just("user-$id")
    }

    StepVerifier.create(result)
      .expectNext("user-1", "unknown-2", "user-3")
      .verifyComplete()
  }

  @Test
  fun `task 06 summarizes positive order lines`() {
    val task = Task06Collect()

    val result = task.summarize(
      Flux.just(
        OrderLine("book", 2, 500),
        OrderLine("pen", 0, 100),
        OrderLine("bag", 3, 700),
      )
    )

    StepVerifier.create(result)
      .expectNext(OrderSummary(totalQuantity = 5, totalPrice = 3100))
      .verifyComplete()
  }

  @Test
  fun `task 07 retries and then returns fallback`() {
    val task = Task07Retry()
    val attempts = AtomicInteger(0)

    val result = task.callWithRetry(
      remoteCall = {
        val attempt = attempts.incrementAndGet()
        if (attempt < 3) Mono.error(IllegalStateException("try again"))
        else Mono.just("ok")
      },
      retryCount = 3,
      fallback = "fallback",
    )

    StepVerifier.create(result)
      .expectNext("ok")
      .verifyComplete()

    kotlin.test.assertEquals(3, attempts.get())
  }

  @Test
  fun `task 07 returns fallback after retries are exhausted`() {
    val task = Task07Retry()

    val result = task.callWithRetry(
      remoteCall = { Mono.error(IllegalStateException("down")) },
      retryCount = 2,
      fallback = "fallback",
    )

    StepVerifier.create(result)
      .expectNext("fallback")
      .verifyComplete()
  }

  @Test
  fun `task 08 emits sum for each batch`() {
    val task = Task08Window()

    val result = task.batchSums(Flux.just(1, 2, 3, 4, 5), batchSize = 2)

    StepVerifier.create(result)
      .expectNext(3, 7, 5)
      .verifyComplete()
  }

  @Test
  fun `task 09 reads request id from context`() {
    val task = Task09Context()

    StepVerifier.create(task.labelWithRequestId("created").contextWrite { it.put("requestId", "r-42") })
      .expectNext("r-42:created")
      .verifyComplete()

    StepVerifier.create(task.labelWithRequestId("created"))
      .expectNext("missing:created")
      .verifyComplete()
  }

  @Test
  fun `task 10 processes payments in order`() {
    val task = Task10Pipeline()
    val charged = mutableListOf<String>()

    val result = task.processPayments(
      payments = Flux.just(
        RawPayment("p1", "u1", 100),
        RawPayment("p2", "u2", 200),
        RawPayment("p3", "u3", -5),
        RawPayment("p4", "u4", 300),
      ),
      loadProfile = { userId -> Mono.just(UserProfile(userId, blocked = userId == "u2")) },
      charge = { payment ->
        if (payment.id == "p4") Mono.error(IllegalStateException("card declined"))
        else {
          charged += payment.id
          Mono.just(Unit)
        }
      },
    )

    StepVerifier.create(result)
      .expectNext(
        PaymentReceipt("p1", "u1", 100, "charged"),
        PaymentReceipt("p2", "u2", 200, "rejected"),
        PaymentReceipt("p4", "u4", 300, "failed"),
      )
      .verifyComplete()

    kotlin.test.assertEquals(listOf("p1"), charged)
  }

  @Test
  fun `task 11 returns primary display name when it is present`() {
    val task = Task11SwitchIfEmpty()

    StepVerifier.create(task.displayName(Mono.just("Jane")) { Mono.just("Fallback") })
      .expectNext("Jane")
      .verifyComplete()
  }

  @Test
  fun `task 11 uses fallback for blank or empty primary name`() {
    val task = Task11SwitchIfEmpty()

    StepVerifier.create(task.displayName(Mono.just("   ")) { Mono.just("Fallback") })
      .expectNext("Fallback")
      .verifyComplete()

    StepVerifier.create(task.displayName(Mono.empty()) { Mono.just("Fallback") })
      .expectNext("Fallback")
      .verifyComplete()
  }

  @Test
  fun `task 12 returns fallback only on timeout`() {
    val task = Task12Timeout()

    StepVerifier.withVirtualTime {
      task.callWithTimeout(
        remoteCall = Mono.delay(Duration.ofSeconds(5)).thenReturn("ok"),
        timeout = Duration.ofSeconds(1),
        fallback = "timeout",
      )
    }
      .thenAwait(Duration.ofSeconds(1))
      .expectNext("timeout")
      .verifyComplete()

    StepVerifier.create(
      task.callWithTimeout(
        remoteCall = Mono.error(IllegalStateException("boom")),
        timeout = Duration.ofSeconds(1),
        fallback = "timeout",
      )
    )
      .expectErrorMatches { it is IllegalStateException && it.message == "boom" }
      .verify()
  }

  @Test
  fun `task 13 keeps first login per user`() {
    val task = Task13Distinct()

    val result = task.firstLoginPerUser(
      Flux.just(
        LoginEvent("u1", "phone"),
        LoginEvent("u2", "laptop"),
        LoginEvent("u1", "tablet"),
        LoginEvent("u3", "desktop"),
        LoginEvent("u2", "watch"),
      )
    )

    StepVerifier.create(result)
      .expectNext(
        LoginEvent("u1", "phone"),
        LoginEvent("u2", "laptop"),
        LoginEvent("u3", "desktop"),
      )
      .verifyComplete()
  }

  @Test
  fun `task 14 calculates totals by currency`() {
    val task = Task14GroupBy()

    val result = task.totalsByCurrency(
      Flux.just(
        MoneyOperation("o1", "USD", 100),
        MoneyOperation("o2", "EUR", 70),
        MoneyOperation("o3", "USD", -30),
        MoneyOperation("o4", "EUR", 0),
        MoneyOperation("o5", "USD", 10),
      )
    )

    StepVerifier.create(result.sort(compareBy { it.currency }))
      .expectNext(CurrencyTotal("EUR", 70), CurrencyTotal("USD", 80))
      .verifyComplete()
  }

  @Test
  fun `task 15 merges system and user feeds`() {
    val task = Task15Merge()

    val result = task.mergeFeeds(
      systemMessages = Flux.just("maintenance"),
      userMessages = Flux.just("hello", "bye"),
    )

    StepVerifier.create(result.sort(compareBy<FeedItem> { it.source }.thenBy { it.text }))
      .expectNext(
        FeedItem("system", "maintenance"),
        FeedItem("user", "bye"),
        FeedItem("user", "hello"),
      )
      .verifyComplete()
  }

  @Test
  fun `task 16 limits concurrent detail loading`() {
    val task = Task16FlatMapConcurrency()
    val active = AtomicInteger(0)
    val maxActive = AtomicInteger(0)

    val result = task.loadDetails(Flux.just(1, 2, 3, 4), maxConcurrency = 2) { id ->
      Mono.defer {
        val current = active.incrementAndGet()
        maxActive.updateAndGet { previous -> maxOf(previous, current) }
        Mono.delay(Duration.ofMillis(20))
          .map { ProductDetails(id, "product-$id") }
          .doFinally { active.decrementAndGet() }
      }
    }

    StepVerifier.create(result.sort(compareBy { it.id }))
      .expectNext(
        ProductDetails(1, "product-1"),
        ProductDetails(2, "product-2"),
        ProductDetails(3, "product-3"),
        ProductDetails(4, "product-4"),
      )
      .verifyComplete()

    kotlin.test.assertTrue(maxActive.get() <= 2)
  }

  @Test
  fun `task 17 writes audit before issuing token`() {
    val task = Task17Then()
    val events = mutableListOf<String>()

    val result = task.openSession(
      userId = "u1",
      isAllowed = { Mono.just(true) },
      writeAudit = {
        events += "audit:$it"
        Mono.just(Unit)
      },
      issueToken = {
        events += "token:$it"
        Mono.just(SessionToken(it, "t-1"))
      },
    )

    StepVerifier.create(result)
      .expectNext(SessionToken("u1", "t-1"))
      .verifyComplete()

    kotlin.test.assertEquals(listOf("audit:u1", "token:u1"), events)
  }

  @Test
  fun `task 17 returns empty when access is denied`() {
    val task = Task17Then()
    val events = mutableListOf<String>()

    val result = task.openSession(
      userId = "u1",
      isAllowed = { Mono.just(false) },
      writeAudit = {
        events += "audit:$it"
        Mono.just(Unit)
      },
      issueToken = {
        events += "token:$it"
        Mono.just(SessionToken(it, "t-1"))
      },
    )

    StepVerifier.create(result)
      .verifyComplete()

    kotlin.test.assertTrue(events.isEmpty())
  }

  @Test
  fun `task 18 loads all pages`() {
    val task = Task18Expand()
    val loadedPages = mutableListOf<Int>()

    val result = task.loadAllPages(firstPage = 1) { page ->
      loadedPages += page
      Mono.just(
        when (page) {
          1 -> Page(items = listOf("a", "b"), nextPage = 2)
          2 -> Page(items = listOf("c"), nextPage = 5)
          5 -> Page(items = listOf("d", "e"), nextPage = null)
          else -> Page(items = emptyList(), nextPage = null)
        }
      )
    }

    StepVerifier.create(result)
      .expectNext("a", "b", "c", "d", "e")
      .verifyComplete()

    kotlin.test.assertEquals(listOf(1, 2, 5), loadedPages)
  }

  @Test
  fun `task 19 closes connection after successful query`() {
    val task = Task19UsingWhen()
    val closed = mutableListOf<String>()

    val result = task.runQuery(
      openConnection = { Mono.just(ReactiveConnection("c1")) },
      query = { connection -> Mono.just("result:${connection.id}") },
      closeConnection = {
        closed += it.id
        Mono.just(Unit)
      },
    )

    StepVerifier.create(result)
      .expectNext("result:c1")
      .verifyComplete()

    kotlin.test.assertEquals(listOf("c1"), closed)
  }

  @Test
  fun `task 19 closes connection after failed query`() {
    val task = Task19UsingWhen()
    val closed = mutableListOf<String>()

    val result = task.runQuery(
      openConnection = { Mono.just(ReactiveConnection("c1")) },
      query = { Mono.error(IllegalStateException("query failed")) },
      closeConnection = {
        closed += it.id
        Mono.just(Unit)
      },
    )

    StepVerifier.create(result)
      .expectErrorMatches { it is IllegalStateException && it.message == "query failed" }
      .verify()

    kotlin.test.assertEquals(listOf("c1"), closed)
  }

  @Test
  fun `task 20 runs checkout in input order`() {
    val task = Task20CheckoutPipeline()
    val reserved = mutableListOf<String>()
    val charged = mutableListOf<String>()

    val result = task.checkout(
      items = Flux.just(
        CartItem("book", 1, 500),
        CartItem("pen", 0, 20),
        CartItem("bag", 2, 1200),
        CartItem("lamp", 1, 700),
        CartItem("cup", 3, 100),
      ),
      hasStock = { item -> Mono.just(item.sku != "bag") },
      reserve = { item ->
        if (item.sku == "lamp") Mono.error(IllegalStateException("reserve failed"))
        else {
          reserved += item.sku
          Mono.just(Unit)
        }
      },
      charge = { item ->
        if (item.sku == "cup") Mono.error(IllegalStateException("charge failed"))
        else {
          charged += item.sku
          Mono.just(Unit)
        }
      },
    )

    StepVerifier.create(result)
      .expectNext(
        CheckoutResult("book", 1, "paid"),
        CheckoutResult("bag", 2, "out_of_stock"),
        CheckoutResult("lamp", 1, "failed"),
        CheckoutResult("cup", 3, "failed"),
      )
      .verifyComplete()

    kotlin.test.assertEquals(listOf("book", "cup"), reserved)
    kotlin.test.assertEquals(listOf("book"), charged)
  }
}
