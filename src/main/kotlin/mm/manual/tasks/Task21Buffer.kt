package mm.manual.tasks

import reactor.core.publisher.Flux

data class SensorReading(
  val sensorId: String,
  val value: Int,
)

data class ReadingBatch(
  val sensorId: String,
  val total: Int,
)

class Task21Buffer {

  /**
   * Разбей readings на пачки фиксированного размера batchSize.
   * Для каждой пачки верни ReadingBatch:
   * - sensorId возьми из первого элемента пачки;
   * - total посчитай как сумму value внутри пачки.
   *
   * Последняя неполная пачка тоже должна попасть в результат.
   */
  fun batchTotals(
    readings: Flux<SensorReading>,
    batchSize: Int,
  ): Flux<ReadingBatch> =
    readings
      .buffer(batchSize)
      .map { list -> ReadingBatch(list.first().sensorId, list.sumOf { it.value }) }
}
