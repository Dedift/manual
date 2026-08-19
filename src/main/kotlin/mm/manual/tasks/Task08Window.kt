package mm.manual.tasks

import reactor.core.publisher.Flux

class Task08Window {

  /**
   * Разбей входящий поток чисел на окна по batchSize элементов.
   * Верни сумму каждого окна.
   */
  fun batchSums(numbers: Flux<Int>, batchSize: Int): Flux<Int> =
    numbers.buffer(batchSize)
      .map { list -> list.sum() }
//    numbers.window( batchSize)
//      .flatMap { window -> window.reduce(0) { accum, number -> accum + number } }
    //TODO("Task 08: use window or buffer")
}
