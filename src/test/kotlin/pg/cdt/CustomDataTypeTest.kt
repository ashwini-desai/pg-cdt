package pg.cdt

import com.impossibl.postgres.jdbc.PGDataSource
import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestResult
import io.kotlintest.extensions.TopLevelTest
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import norm.executeCommand
import norm.executeQuery
import norm.toList
import java.sql.ResultSet
import java.sql.SQLData
import java.sql.SQLInput
import java.sql.SQLOutput


class UserTest : StringSpec() {

    private val dataSource = PGDataSource().also {
        it.databaseUrl = "jdbc:pgsql://localhost/pg_cdt"
        it.user = "postgres"
    }

    override fun beforeSpecClass(spec: Spec, tests: List<TopLevelTest>) {
        super.beforeSpecClass(spec, tests)
        dataSource.connection.use {
            it.executeCommand("""
                CREATE TYPE address AS (
                    block_no numeric,
                    street_address varchar,
                    city varchar,
                    state varchar,
                    pin_code numeric
                );
            """.trimIndent())
            it.executeCommand("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR,
                    address address
                );
            """.trimIndent())
        }
    }

    override fun afterSpecClass(spec: Spec, results: Map<TestCase, TestResult>) {
        super.afterSpecClass(spec, results)
        dataSource.connection.use {
            it.executeCommand("DROP TABLE users;")
            it.executeCommand("DROP TYPE address;")
        }
    }

    init {
        "fetch all records" {
            dataSource.connection.use {
                val sqlFile = this::class.java.classLoader.getResource("users.sql").readText().trim()
                it.executeCommand(sqlFile)
            }

            val result = dataSource.connection.use {
                it.executeQuery("SELECT * from users;").getColumnAsType(3, Address::class.java)
            }

            val queryAnalysis = dataSource.connection.use {
                it.executeQuery("EXPLAIN ANALYZE SELECT * from users;").toList()
            }

            println("CDT_OBJECT_WITHOUT_INDEX: Plan time = " + queryAnalysis[1])
            println("CDT_OBJECT_WITHOUT_INDEX: Execution time = " + queryAnalysis[2])


            result.size shouldBe 2
            result[0].blockNumber shouldBe 70897
            result[1].blockNumber shouldBe 78126
        }

        "with index" {
            dataSource.connection.use {
                it.executeCommand("SET enable_seqscan to FALSE;")
                it.executeCommand("CREATE UNIQUE INDEX IF NOT EXISTS idx_address ON users (address)")

                val result = dataSource.connection.use {
                    it.executeQuery("SELECT * from users where (address).pin_code = 25130;").toList()
                }

                val queryAnalysis = dataSource.connection.use {
                    it.executeQuery("EXPLAIN ANALYZE SELECT * from users where (address).pin_code = 25130;;").toList()
                }

                println("CDT_OBJECT_WITHOUT_INDEX: Plan time = " + queryAnalysis[3])
                println("CDT_OBJECT_WITHOUT_INDEX: Execution time = " + queryAnalysis[4])

                result.size shouldBe 1
            }
        }
    }
}

data class Address(
        private var blockNo: Int,
        private var streetAddress: String,
        private var city: String,
        private var country: String,
        private var pinCode: Int
) : SQLData {

    constructor() : this(0, "", "", "", 0)

    val blockNumber: Int
        get() = blockNo

    override fun readSQL(stream: SQLInput?, typeName: String?) {
        blockNo = stream?.readInt()!!
        streetAddress = stream.readString()
        city = stream.readString()
        country = stream.readString()
        pinCode = stream.readInt()
    }

    override fun getSQLTypeName(): String = "public.address"


    override fun writeSQL(stream: SQLOutput?) {
        stream?.writeInt(blockNo)!!
        stream.writeString(streetAddress)
        stream.writeString(city)
        stream.writeString(country)
        stream.writeInt(pinCode)
    }
}

fun <T : Any> ResultSet.getColumnAsType(columnIndex: Int, type: Class<T>): List<T> =
        this.use { generateSequence { if (this.next()) this.getObject(3, type) else null }.toList() }