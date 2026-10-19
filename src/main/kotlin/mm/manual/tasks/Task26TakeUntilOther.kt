package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class Task26TakeUntilOther {

  /**
   * Верни события из events, пока не сработает stopSignal.
   * Как только stopSignal emit-ит Unit, поток events должен остановиться.
   */
  fun eventsUntilStop(
    events: Flux<String>,
    stopSignal: Mono<Unit>,
  ): Flux<String> =
    events.takeUntilOther (stopSignal)
}
