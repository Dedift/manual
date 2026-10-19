package mm.manual.tasks

import reactor.core.publisher.Flux

data class BalanceChange(
  val amount: Long,
)

data class BalanceSnapshot(
  val balance: Long,
)

class Task23Scan {

  /**
   * Посчитай историю баланса.
   * initialBalance - стартовое значение.
   * Каждое изменение amount нужно прибавить к предыдущему балансу.
   *
   * Верни только новые снимки баланса после каждого изменения.
   * Стартовый initialBalance в результат включать не нужно.
   */
  fun balanceHistory(
    changes: Flux<BalanceChange>,
    initialBalance: Long,
  ): Flux<BalanceSnapshot> =
    changes.scan(BalanceSnapshot(initialBalance)) { currentBalance, change ->
      BalanceSnapshot(currentBalance.balance + change.amount)
    }
      .skip(1)
}
