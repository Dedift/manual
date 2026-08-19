package mm.manual.tasks

import reactor.core.publisher.Mono

class Task07Retry {

  /**
   * Вызови remoteCall().
   * При ошибке повтори вызов максимум retryCount раз.
   * Если все попытки завершились ошибкой, верни fallback.
   */
  fun callWithRetry(
    remoteCall: () -> Mono<String>,
    retryCount: Long,
    fallback: String,
  ): Mono<String> =
    remoteCall()
      .retry(retryCount)
      .onErrorReturn(fallback)
    //TODO("Task 07: use retry and onErrorReturn/onErrorResume")
}
