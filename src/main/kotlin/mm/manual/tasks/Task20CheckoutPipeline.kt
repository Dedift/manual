package mm.manual.tasks

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

data class CartItem(
  val sku: String,
  val quantity: Int,
  val price: Long,
)

data class CheckoutResult(
  val sku: String,
  val quantity: Int,
  val status: String,
)

class Task20CheckoutPipeline {

  /**
   * Для каждого item:
   * 1. Игнорируй quantity <= 0.
   * 2. Вызови hasStock(item). Если товара нет, верни status = "out_of_stock".
   * 3. Если товар есть, вызови reserve(item), затем charge(item).
   * 4. Если reserve или charge упали, верни status = "failed".
   * 5. Если все успешно, верни status = "paid".
   *
   * Порядок результатов должен совпадать с порядком входных товаров.
   */
  fun checkout(
    items: Flux<CartItem>,
    hasStock: (CartItem) -> Mono<Boolean>,
    reserve: (CartItem) -> Mono<Unit>,
    charge: (CartItem) -> Mono<Unit>,
  ): Flux<CheckoutResult> =
    items
      .filter { item -> item.quantity > 0 }
      .concatMap { item -> // Использование concatMap гарантирует сохранение порядка результатов
        hasStock(item)
          .flatMap { isAvailable ->
            if (!isAvailable) {
              // Товара нет на складе — сразу возвращаем "out_of_stock"
              Mono.just(CheckoutResult(item.sku, item.quantity, "out_of_stock"))
            } else {
              // Товар есть — запускаем резерв, затем списание денег
              reserve(item)
                .then(Mono.defer { charge(item) })
                .map { CheckoutResult(item.sku, item.quantity, "paid") }
                // Локально перехватываем ошибку шага резерва или чарджа
                .onErrorReturn(CheckoutResult(item.sku, item.quantity, "failed"))
            }
          }
      }
    //TODO("Task 20: build ordered checkout pipeline")
}
