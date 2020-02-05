package pg.cdt

import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get

@Controller("/persons")
class PersonController() {

    @Get
    fun get() =
            listOf(
                    Person("Julie", 23,
                            Address(201, "Bartelt Junction", "Duke", "NY", 51023)))
}