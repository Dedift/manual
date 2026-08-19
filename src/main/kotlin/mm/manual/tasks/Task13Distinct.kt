package mm.manual.tasks

import reactor.core.publisher.Flux

data class LoginEvent(
  val userId: String,
  val deviceId: String,
)

class Task13Distinct {

  /**
   * Верни только первое событие для каждого userId.
   * Повторные события того же пользователя нужно игнорировать.
   */
  fun firstLoginPerUser(events: Flux<LoginEvent>): Flux<LoginEvent> =
    events.distinct { event -> event.userId }
    //TODO("Task 13: use distinct by key")
}
