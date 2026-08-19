package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class Task02FlatMap {

  /**
   * Для каждого userId вызови loadRoles(userId) и верни строки вида "userId:role".
   * Порядок элементов сохранять не требуется.
   */
  fun userRoleLabels(
    userIds: Flux<String>,
    loadRoles: (String) -> Mono<List<String>>,
  ): Flux<String> =
    userIds
      .flatMap { userId -> loadRoles(userId)
        .flatMapIterable { roles -> roles.map {role -> "$userId:$role"} }}
}
