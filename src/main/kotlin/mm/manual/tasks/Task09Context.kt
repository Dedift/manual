package mm.manual.tasks

import reactor.core.publisher.Mono

class Task09Context {

  /**
   * Достань requestId из Reactor Context по ключу "requestId".
   * Верни строку "$requestId:$message".
   * Если requestId отсутствует, используй "missing".
   */
  fun labelWithRequestId(message: String): Mono<String> =
    Mono.deferContextual { ctx ->
      val requestId = ctx.getOrDefault("requestId", "missing")
      Mono.just("$requestId:$message")
    }
    //TODO("Task 09: read Reactor Context")
}
