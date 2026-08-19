package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

data class AuditRecord(
  val id: Int,
  val status: String,
)

class Task03ConcatMap {

  /**
   * Обработай ids строго по порядку.
   * Для каждого id вызови save(id) и верни результат.
   */
  fun saveSequentially(
    ids: Flux<Int>,
    save: (Int) -> Mono<AuditRecord>,
  ): Flux<AuditRecord> =
    ids.concatMap { id -> save(id) }
    //TODO("Task 03: use concatMap")
}
