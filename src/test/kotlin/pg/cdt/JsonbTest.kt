package pg.cdt

import com.impossibl.postgres.jdbc.PGDataSource
import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestResult
import io.kotlintest.extensions.TopLevelTest
import io.kotlintest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotlintest.specs.DescribeSpec
import norm.executeCommand
import norm.executeQuery
import norm.toList


class JsonbTest : DescribeSpec() {
    private val dataSource = PGDataSource().also {
        it.databaseUrl = "jdbc:pgsql://localhost/pg_cdt"
        it.user = "postgres"
    }

    override fun beforeSpecClass(spec: Spec, tests: List<TopLevelTest>) {
        super.beforeSpecClass(spec, tests)
        dataSource.connection.use {
            it.executeCommand("""
                CREATE TABLE IF NOT EXISTS contacts (
                                email VARCHAR PRIMARY KEY,
                                name VARCHAR,
                                phone_numbers JSONB
                            );
            """.trimIndent())
        }
    }

    override fun afterSpecClass(spec: Spec, results: Map<TestCase, TestResult>) {
        super.afterSpecClass(spec, results)
        dataSource.connection.use {
            it.executeCommand("DROP TABLE contacts;")
        }
    }

    init {
        describe("with one phone number as json object") {

            it("without index") {
                dataSource.connection.use {
                    it.executeCommand("INSERT INTO contacts values ('julie@xyz.com', 'Julie Dsouza', '{\"Home\":8899776612}');");
                    it.executeCommand("INSERT INTO contacts values ('lthain9@gov.uk', 'Luise Thain', '{\"Work\":9876543210}');");
                }

                val result = dataSource.connection.use {
                    it.executeQuery("SELECT phone_numbers from contacts;").toList()
                }

                result.map { it["phone_numbers"] } shouldContainExactlyInAnyOrder listOf("{\"Home\": 8899776612}", "{\"Work\": 9876543210}")
            }
        }
    }

}