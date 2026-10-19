package mm.manual.tasks

import reactor.core.publisher.Flux

data class PriceTick(
  val sku: String,
  val price: Long,
)

data class StockTick(
  val sku: String,
  val available: Boolean,
)

data class CatalogState(
  val sku: String,
  val price: Long,
  val available: Boolean,
)

class Task24CombineLatest {

  /**
   * Объедини последние price и stock в CatalogState.
   * Результат можно emit-ить только когда уже есть хотя бы одно значение из обоих потоков.
   *
   * В тестах оба потока относятся к одному sku.
   */
  fun catalogStates(
    prices: Flux<PriceTick>,
    stocks: Flux<StockTick>,
  ): Flux<CatalogState> =
    Flux.combineLatest(prices, stocks) {
      price, stock -> CatalogState(price.sku, price.price, stock.available)
    }
}
