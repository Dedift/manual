package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

data class ProductDetails(
  val id: Int,
  val name: String,
)

class Task16FlatMapConcurrency {

  /**
   * Для каждого productId вызови loadDetails.
   * Одновременно должно выполняться не больше maxConcurrency загрузок.
   * Порядок результата сохранять не требуется.
   */
  fun loadDetails(
    productIds: Flux<Int>,
    maxConcurrency: Int,
    loadDetails: (Int) -> Mono<ProductDetails>,
  ): Flux<ProductDetails> =
    productIds
      .flatMap({ id -> loadDetails(id).publishOn(Schedulers.parallel()) }, maxConcurrency)
    //TODO("Task 16: use flatMap with maxConcurrency")
}
