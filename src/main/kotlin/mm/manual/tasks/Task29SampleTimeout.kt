package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

data class SearchInput(
  val text: String,
)

class Task29SampleTimeout {

  /**
   * На вход приходит поток изменений поисковой строки.
   * Нужно вернуть только последнее значение после паузы debounce.
   * Пустые и blank-строки нужно отфильтровать до debounce.
   */
  fun debouncedSearches(
    inputs: Flux<SearchInput>,
    debounce: Duration,
  ): Flux<String> =
    inputs.filter { inputs -> inputs.text.isNotBlank() }
      .sampleTimeout { Mono.delay(debounce) }
      .map { inputs -> inputs.text }
}
