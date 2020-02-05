package pg.cdt

data class Person(
        val name: String,
        val age: Int,
        val address: Address
)

data class Address(
        val flatNo: Int,
        val streetName: String,
        val city: String,
        val state: String,
        val pinCode: Int
)
