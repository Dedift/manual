package mm.manual.tasks

import reactor.core.publisher.Mono

data class Account(
  val id: String,
  val owner: String,
)

data class Balance(
  val accountId: String,
  val amount: Long,
)

data class AccountView(
  val id: String,
  val owner: String,
  val balance: Long,
)

class Task04Zip {

  /**
   * Одновременно загрузи аккаунт и баланс.
   * Верни AccountView из двух результатов.
   */
  fun accountView(
    accountId: String,
    loadAccount: (String) -> Mono<Account>,
    loadBalance: (String) -> Mono<Balance>,
  ): Mono<AccountView> =
    loadBalance(accountId)
      .zipWith(loadAccount(accountId)) {
        balance, account -> AccountView(accountId, account.owner, balance.amount)
      }
    //TODO("Task 04: use Mono.zip")
}
