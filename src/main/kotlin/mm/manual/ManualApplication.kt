package mm.manual

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ManualApplication

fun main(args: Array<String>) {
  runApplication<ManualApplication>(*args)
}
