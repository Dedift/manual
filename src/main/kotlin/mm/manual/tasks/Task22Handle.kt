package mm.manual.tasks

import reactor.core.publisher.Flux

class Task22Handle {

  /**
   * На вход приходит поток строк.
   * Нужно вернуть только корректные положительные числа.
   *
   * Правила:
   * - строку сначала trim();
   * - если строка не парсится в Int, пропусти ее;
   * - если число <= 0, пропусти его;
   * - остальные числа верни в исходном порядке.
   */
  fun positiveNumbers(source: Flux<String>): Flux<Int> =
    source.handle { string, sink ->
      val trimmed = string.trim()
      val number = trimmed.toIntOrNull()
      if (number != null && number > 0) {
        sink.next(number)
      }
    }
}
