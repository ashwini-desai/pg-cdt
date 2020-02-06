package pg.cdt.contact

data class Contact(
        val name: String,
        val email: String,
        val phoneNumbers: Map<String, Long>
)