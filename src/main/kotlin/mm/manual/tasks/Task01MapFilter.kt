package mm.manual.tasks

import reactor.core.publisher.Flux

data class UserEvent(
  val userId: String,
  val type: String,
  val amount: Int,
)

class Task01MapFilter {

  /**
   * Оставь только события с type == "PAID" и amount > 0.
   * Верни поток сумм в копейках: amount * 100.
   */
  fun paidAmountsInCents(events: Flux<UserEvent>): Flux<Int> =
    events.filter { event -> event.type == "PAID" && event.amount > 0 }
      .map { event -> event.amount * 100 }
}
