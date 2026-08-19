package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class Task05ErrorHandling {

  /**
   * Для каждого id вызови loadName(id).
   * Если загрузка конкретного имени завершилась ошибкой, верни "unknown-$id" только для этого id.
   * Ошибка одного элемента не должна ломать весь поток.
   */
  fun loadNamesWithFallback(
    ids: Flux<Int>,
    loadName: (Int) -> Mono<String>,
  ): Flux<String> =
    ids.flatMap { id -> loadName(id).onErrorReturn("unknown-$id") }
    //TODO("Task 05: handle per-item errors")
}
