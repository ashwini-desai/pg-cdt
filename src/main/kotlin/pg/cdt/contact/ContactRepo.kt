package pg.cdt.contact

import com.fasterxml.jackson.core.io.JsonStringEncoder
import com.fasterxml.jackson.databind.ObjectMapper
import norm.*
import javax.inject.Inject
import javax.inject.Singleton
import javax.sql.DataSource

@Singleton
class ContactRepo(@Inject private val dataSource: DataSource) {

    fun persist(contacts: List<Contact>): Int {
        val result = dataSource.connection.use { connection ->
            connection.batchExecuteCommand("INSERT INTO contacts VALUES (?, ?, ?)",
                    contacts.map {
                        val encoder = JsonStringEncoder.getInstance()
                        listOf(
                                it.name,
                                it.email,
                                "\"${String(encoder.quoteAsString(ObjectMapper().writeValueAsString(it.phoneNumbers)))}\"")
                    })
        }
        return result.size
    }

    fun fetchAll() = dataSource.connection.use { connection ->
        val result = connection.executeQuery("SELECT * from contacts")
        result.toList()
    }
}