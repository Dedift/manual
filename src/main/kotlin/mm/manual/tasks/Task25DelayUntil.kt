package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

data class OutboxMessage(
  val id: String,
  val payload: String,
)

class Task25DelayUntil {

  /**
   * Для каждого message сначала вызови saveAudit(message).
   * Сам message можно вернуть дальше только после успешного завершения saveAudit.
   *
   * Если saveAudit упал, ошибка должна пройти дальше, а message не должен быть emit-нут.
   */
  fun emitAfterAudit(
    messages: Flux<OutboxMessage>,
    saveAudit: (OutboxMessage) -> Mono<Unit>,
  ): Flux<OutboxMessage> =
    messages.delayUntil { messages -> saveAudit(messages).thenReturn(messages) }
}
