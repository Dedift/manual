package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

data class RawPayment(
  val id: String,
  val userId: String,
  val amount: Long,
)

data class UserProfile(
  val userId: String,
  val blocked: Boolean,
)

data class PaymentReceipt(
  val paymentId: String,
  val userId: String,
  val amount: Long,
  val status: String,
)

class Task10Pipeline {

  /**
   * Для каждого платежа:
   * 1. Игнорируй amount <= 0.
   * 2. Загрузи профиль пользователя.
   * 3. Если пользователь blocked, верни receipt со status = "rejected".
   * 4. Иначе вызови charge(payment) и верни receipt со status = "charged".
   * 5. Если charge(payment) упал, верни receipt со status = "failed".
   *
   * Порядок платежей в выходном потоке должен совпадать с входным.
   */
  fun processPayments(
    payments: Flux<RawPayment>,
    loadProfile: (String) -> Mono<UserProfile>,
    charge: (RawPayment) -> Mono<Unit>,
  ): Flux<PaymentReceipt> =
    payments
      .filter { payment -> payment.amount > 0 }
      .concatMap { payment -> loadProfile(payment.userId)
        .flatMap { profile ->
          if (profile.blocked) {
            Mono.just(PaymentReceipt(payment.id, profile.userId, payment.amount, "rejected"))
          } else{
            charge(payment)
              .map { PaymentReceipt(payment.id, profile.userId, payment.amount, "charged") }
              .onErrorReturn(PaymentReceipt(payment.id, profile.userId, payment.amount, "failed"))
          }
        }
      }
        //TODO("Task 10: build the full reactive pipeline")
}
