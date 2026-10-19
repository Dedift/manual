package mm.manual.tasks

import reactor.core.publisher.Mono

class Task28Cache {

  /**
   * Верни Mono, который лениво вызывает loadValue(), но кеширует успешный результат.
   * Если на возвращенный Mono подпишутся несколько раз, loadValue должен быть вызван один раз.
   */
  fun cachedValue(loadValue: () -> Mono<String>): Mono<String> =
    Mono.defer { loadValue() }.cache()
}
