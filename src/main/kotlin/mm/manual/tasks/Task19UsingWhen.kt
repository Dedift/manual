package mm.manual.tasks

import reactor.core.publisher.Mono

data class ReactiveConnection(
  val id: String,
)

class Task19UsingWhen {

  /**
   * Открой соединение через openConnection.
   * Передай его в query.
   * Соединение нужно закрыть через closeConnection и при успехе, и при ошибке query.
   */
  fun runQuery(
    openConnection: () -> Mono<ReactiveConnection>,
    query: (ReactiveConnection) -> Mono<String>,
    closeConnection: (ReactiveConnection) -> Mono<Unit>,
  ): Mono<String> =
    Mono.usingWhen(
      Mono.defer { openConnection() },               // 1. Открываем ресурс лениво
      { connection -> query(connection) },          // 2. Выполняем запрос
      { connection -> closeConnection(connection) } // 3. Закрываем ресурс (и при успехе, и при ошибке)
    )


    //TODO("Task 19: use Mono.usingWhen")
}
