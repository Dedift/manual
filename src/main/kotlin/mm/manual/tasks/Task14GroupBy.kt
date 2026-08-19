package mm.manual.tasks

import reactor.core.publisher.Flux

data class MoneyOperation(
  val id: String,
  val currency: String,
  val amount: Long,
)

data class CurrencyTotal(
  val currency: String,
  val amount: Long,
)

class Task14GroupBy {

  /**
   * Сгруппируй операции по currency и посчитай сумму amount внутри каждой группы.
   * Операции с amount == 0 тоже участвуют, отрицательные суммы вычитаются.
   */
  fun totalsByCurrency(operations: Flux<MoneyOperation>): Flux<CurrencyTotal> =
    operations.groupBy { it.currency }
      .flatMap { groupedFlux ->
        groupedFlux.reduce(CurrencyTotal(groupedFlux.key(), 0))
        {
          accum, operation -> CurrencyTotal(groupedFlux.key(), accum.amount + operation.amount)
        }
      }
//TODO("Task 14: use groupBy and reduce")
}
