package pg.cdt.person

import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import pg.cdt.person.Address
import pg.cdt.person.Person

@Controller("/persons")
class PersonController() {

    @Get
    fun get() =
            listOf(
                    Person("Julie", 23,
                            Address(201, "Bartelt Junction", "Duke", "NY", 51023)))
}