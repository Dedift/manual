package mm.manual.tasks

import reactor.core.publisher.Mono
import java.time.Duration

class Task12Timeout {

  /**
   * Вызови remoteCall.
   * Если он не успел завершиться за timeout, верни fallback.
   * Если remoteCall завершился другой ошибкой, эту ошибку нужно пробросить дальше.
   */
  fun callWithTimeout(
    remoteCall: Mono<String>,
    timeout: Duration,
    fallback: String,
  ): Mono<String> =
    remoteCall
      .timeout(timeout, Mono.just(fallback))
    //TODO("Task 12: use timeout with fallback")
}
