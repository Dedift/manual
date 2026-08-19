package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

data class OrderLine(
  val product: String,
  val quantity: Int,
  val price: Long,
)

data class OrderSummary(
  val totalQuantity: Int,
  val totalPrice: Long,
)

class Task06Collect {

  /**
   * Посчитай общий quantity и общий price.
   * Строки с quantity <= 0 игнорируй.
   * totalPrice = sum(quantity * price).
   */
  fun summarize(lines: Flux<OrderLine>): Mono<OrderSummary> =
    lines
      .filter { line -> line.quantity > 0 }
      .reduce(OrderSummary(0, 0)) {
        accum, line ->
        OrderSummary(accum.totalQuantity + line.quantity,
          accum.totalPrice + (line.quantity * line.price)) }

    //TODO("Task 06: filter and reduce/collect")
}
