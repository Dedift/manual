package mm.manual.tasks

import reactor.core.publisher.Flux

data class FeedItem(
  val source: String,
  val text: String,
)

class Task15Merge {

  /**
   * Объедини системные и пользовательские сообщения в один поток.
   * Для системных сообщений source должен быть "system", для пользовательских - "user".
   * Порядок между источниками сохранять не нужно: быстрее пришедшее сообщение может прийти раньше.
   */
  fun mergeFeeds(
    systemMessages: Flux<String>,
    userMessages: Flux<String>,
  ): Flux<FeedItem> =
//    systemMessages.map { FeedItem("system", it) }
//      .mergeWith (userMessages.map { FeedItem("user", it)})
    Flux.merge(
      systemMessages.map { FeedItem("system", it) },
      userMessages.map { FeedItem("user", it) }
    )
    //TODO("Task 15: map both sources and merge them")
}
