package pg.cdt.contact

import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import javax.inject.Inject


@Controller("/contacts")
class ContactController(@Inject private val contactRepo: ContactRepo) {

    @Get
    fun get() = contactRepo.fetchAll()

    @Post
    fun save(contacts: List<Contact>) = contactRepo.persist(contacts)

}