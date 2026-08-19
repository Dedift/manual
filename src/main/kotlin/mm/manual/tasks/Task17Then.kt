package mm.manual.tasks

import reactor.core.publisher.Mono

data class SessionToken(
  val userId: String,
  val token: String,
)

class Task17Then {

  /**
   * Сначала вызови isAllowed(userId).
   * Если доступ запрещен, верни Mono.empty().
   * Если доступ разрешен, вызови writeAudit(userId), дождись завершения и только потом вызови issueToken(userId).
   */
  fun openSession(
    userId: String,
    isAllowed: (String) -> Mono<Boolean>,
    writeAudit: (String) -> Mono<Unit>,
    issueToken: (String) -> Mono<SessionToken>,
  ): Mono<SessionToken> =
    isAllowed(userId)
      .filter { allowed -> allowed }
      .flatMap {
        writeAudit(userId)
          .then(Mono.defer { issueToken(userId) })
      }
//TODO("Task 17: use filter, flatMap, then")
}
