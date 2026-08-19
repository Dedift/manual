package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

data class Page<T>(
  val items: List<T>,
  val nextPage: Int?,
)

class Task18Expand {

  /**
   * Загрузи первую страницу с номером firstPage, затем все следующие страницы по nextPage.
   * Верни один поток всех items в порядке страниц.
   */
  fun <T : Any> loadAllPages(
    firstPage: Int,
    loadPage: (Int) -> Mono<Page<T>>,
  ): Flux<T> =
    Mono.just(firstPage)
      .flatMap { page -> loadPage(page) }
      .expand { page ->
        if (page.nextPage != null){
          loadPage(page.nextPage)
        } else{
          Mono.empty()
        }
      }
      .flatMapIterable { page -> page.items }
//TODO("Task 18: use expand and flatMapIterable")
}
