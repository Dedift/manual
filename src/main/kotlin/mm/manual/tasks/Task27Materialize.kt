package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

data class LoadReport(
  val values: List<String>,
  val errorMessage: String?,
)

class Task27Materialize {

  /**
   * Собери отчет по source.
   * Если source завершился успешно, верни values и errorMessage = null.
   * Если source завершился ошибкой, верни все значения, которые успели прийти до ошибки,
   * и errorMessage из этой ошибки.
   *
   * Ошибка не должна падать наружу из результата.
   */
  fun report(source: Flux<String>): Mono<LoadReport> =
    source
      .materialize()
      .collectList()
      .map { signals ->
        val values = signals.filter { it.isOnNext }.mapNotNull { it.get() }
        val errorMessage = signals.firstOrNull { it.isOnError }?.throwable?.message
        LoadReport(values, errorMessage)
      }
}
