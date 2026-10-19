package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

data class NotificationCommand(
  val userId: String,
  val channel: String,
  val text: String,
)

data class UserSettings(
  val userId: String,
  val mutedChannels: Set<String>,
)

data class NotificationResult(
  val userId: String,
  val channel: String,
  val status: String,
)

class Task30NotificationPipeline {

  /**
   * Для каждой command:
   * 1. Игнорируй команды с blank text.
   * 2. Загрузи настройки пользователя через loadSettings(userId).
   * 3. Если command.channel находится в mutedChannels, верни status = "muted".
   * 4. Иначе вызови send(command) и при успехе верни status = "sent".
   * 5. Если loadSettings или send упали, верни status = "failed" только для этой command.
   *
   * Порядок результатов должен совпадать с порядком входных command.
   */
  fun process(
    commands: Flux<NotificationCommand>,
    loadSettings: (String) -> Mono<UserSettings>,
    send: (NotificationCommand) -> Mono<Unit>,
  ): Flux<NotificationResult> =
    commands.filter { it.text.isNotBlank() }
      .concatMap { command ->
        loadSettings(command.userId)
          .flatMap { settings ->
            if (command.channel in settings.mutedChannels) {
              Mono.just(NotificationResult(command.userId, command.channel, "muted"))
            } else {
              send(command).thenReturn(NotificationResult(command.userId, command.channel, "sent"))
            }
          }.onErrorReturn(NotificationResult(command.userId, command.channel, "failed"))
      }
}
