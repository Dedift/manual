package mm.manual.tasks

import reactor.core.publisher.Mono

class Task11SwitchIfEmpty {

  /**
   * Верни значение из primary, если оно есть и не blank.
   * Если primary пустой или вернул blank-строку, вызови fallback и верни его значение.
   */
  fun displayName(
    primary: Mono<String>,
    fallback: () -> Mono<String>,
  ): Mono<String> =
    primary
      .filter { primary -> primary.isNotBlank() }
      .switchIfEmpty(Mono.defer { fallback() })
  //TODO("Task 11: use filter and switchIfEmpty")
}
