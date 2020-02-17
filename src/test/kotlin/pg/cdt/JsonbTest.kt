@file:Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")

package pg.cdt

import com.impossibl.postgres.jdbc.PGDataSource
import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.TestResult
import io.kotlintest.extensions.TopLevelTest
import io.kotlintest.matchers.collections.shouldContainAll
import io.kotlintest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotlintest.shouldBe
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
        describe("phone_numbers as json object") {

            it("without index") {
                dataSource.connection.use {
                    val sqlFile = this::class.java.classLoader.getResource("contacts.sql").readText().trim()
                    it.executeCommand(sqlFile)
                }

                val result = dataSource.connection.use {
                    it.executeQuery("SELECT phone_numbers from contacts;").toList()
                }

                val queryAnalysis = dataSource.connection.use {
                    it.executeQuery("EXPLAIN ANALYZE SELECT phone_numbers from contacts;").toList()
                }

                println("JSON_OBJECT_WITHOUT_INDEX: Plan time = " + queryAnalysis[1])
                println("JSON_OBJECT_WITHOUT_INDEX: Execution time = " + queryAnalysis[2])

                result.map { it["phone_numbers"] } shouldContainAll listOf("{\"Home\": 8899776612}", "{\"Work\": 9876543210}")
            }

            it("with index: ") {
                dataSource.connection.use {
                    it.executeCommand("CREATE INDEX IF NOT EXISTS idx_phone_numbers ON contacts USING gin (phone_numbers);")
                }

                val result = dataSource.connection.use {
                    it.executeQuery("SELECT phone_numbers from contacts where phone_numbers ?? 'Home';").toList()
                }

                val queryAnalysis = dataSource.connection.use {
                    it.executeCommand("SET enable_seqscan to FALSE;")
                    it.executeQuery("EXPLAIN ANALYZE SELECT phone_numbers from contacts where phone_numbers ?? 'Home';").toList()
                }

                println(">> " + queryAnalysis[0])
                println("JSON_OBJECT_WITH_INDEX: Plan time = " + queryAnalysis[3])
                println("JSON_OBJECT_WITH_INDEX: Execution time = " + queryAnalysis[4])

                result.map { it["phone_numbers"] } shouldContainExactlyInAnyOrder listOf("{\"Home\": 8899776612}", "{\"Home\": 9876543210}")
            }
        }

        describe("phone_numbers as json array") {
            it("without index") {
                dataSource.connection.use {
                    val sqlFile = this::class.java.classLoader.getResource("contacts_with_multiple_phone_numbers.sql").readText().trim()
                    it.executeCommand("DROP INDEX idx_phone_numbers;")
                    it.executeCommand("DELETE FROM contacts;")
                    it.executeCommand(sqlFile)
                }

                val result = dataSource.connection.use {
                    it.executeQuery("SELECT phone_numbers from contacts;").toList()
                }

                val queryAnalysis = dataSource.connection.use {
                    it.executeQuery("EXPLAIN ANALYZE SELECT phone_numbers from contacts;").toList()
                }

                println("JSON_ARRAY_WITHOUT_INDEX: Plan time = " + queryAnalysis[1])
                println("JSON_ARRAY_WITHOUT_INDEX: Execution time = " + queryAnalysis[2])

                result.map { it["phone_numbers"] }.size shouldBe 20
            }

            it("zero results: -> and ->> cannot be used to query values inside array of json objects. unnesting is the only way") {
                val result = dataSource.connection.use {
                    it.executeQuery("SELECT phone_numbers from contacts where phone_numbers->>'tag' = '\"Home\"';").toList()
                }

                val queryAnalysis = dataSource.connection.use {
                    it.executeCommand("SET enable_seqscan to FALSE;")
                    it.executeQuery("EXPLAIN ANALYZE SELECT phone_numbers from contacts where phone_numbers->>'tag' = '\"Home\"';").toList()
                }

                println(">> " + queryAnalysis[0])
                println("JSON_ARRAY_WITH_SUB_OBJECT_KEY_INDEX: Plan time = " + queryAnalysis[3])
                println("JSON_ARRAY_WITH_SUB_OBJECT_KEY_INDEX: Execution time = " + queryAnalysis[4])

                result.map { it["phone_numbers"] }.size shouldBe 0
            }

            it("with index: will not use index without where clause") {
                dataSource.connection.use {
                    it.executeCommand("CREATE INDEX idx_phone_numbers ON contacts USING gin (phone_numbers);")
                }

                val result = dataSource.connection.use {
                    it.executeQuery("SELECT phone_numbers from contacts;").toList()
                }

                val queryAnalysis = dataSource.connection.use {
                    it.executeQuery("EXPLAIN ANALYZE SELECT phone_numbers from contacts;").toList()
                }

                println("JSON_ARRAY_WITH_INDEX: Plan time = " + queryAnalysis[1])
                println("JSON_ARRAY_WITH_INDEX: Execution time = " + queryAnalysis[2])

                result.map { it["phone_numbers"] }.size shouldBe 20
            }

            it("with index: while querying based on value of sub-object's key") {
                val result = dataSource.connection.use {
                    it.executeQuery("SELECT phone_numbers from contacts where phone_numbers @> '[{\"tag\":\"Home\"}]';").toList()
                }

                val queryAnalysis = dataSource.connection.use {
                    it.executeCommand("SET enable_seqscan to FALSE;")
                    it.executeQuery("EXPLAIN ANALYZE SELECT phone_numbers from contacts where phone_numbers @> '[{\"tag\":\"Home\"}]';").toList()
                }

                println(">> " + queryAnalysis[0])
                println("JSON_ARRAY_WITH_SUB_OBJECT_KEY_INDEX: Plan time = " + queryAnalysis[3])
                println("JSON_ARRAY_WITH_SUB_OBJECT_KEY_INDEX: Execution time = " + queryAnalysis[4])

                result.map { it["phone_numbers"] }.size shouldBe 10
            }

            it("with index: will not use index because of 'join lateral/unnesting', selecting sub-object's particular key's value") {
               val result = dataSource.connection.use {
                    it.executeQuery("""SELECT obj.val->>'value' as number
                                           FROM   contacts
                                           JOIN   LATERAL jsonb_array_elements(contacts.phone_numbers) obj(val) ON obj.val->>'tag' = 'Work'
                                           WHERE  contacts.phone_numbers @> '[{"tag":"Work"}]';""").toList()
                }

                val queryAnalysis = dataSource.connection.use {
                    it.executeCommand("SET enable_seqscan to FALSE;")
                    it.executeQuery("EXPLAIN ANALYZE SELECT obj.val->>'value' as number FROM contacts JOIN LATERAL jsonb_array_elements(contacts.phone_numbers) obj(val) ON obj.val->>'tag' = 'Work' WHERE contacts.phone_numbers @> '[{\"tag\":\"Work\"}]';").toList()
                }

                println(">> " + queryAnalysis[0])
                println("JSON_ARRAY_WITH_SUB_OBJECT_KEY_INDEX: Plan time = " + queryAnalysis[6])
                println("JSON_ARRAY_WITH_SUB_OBJECT_KEY_INDEX: Execution time = " + queryAnalysis[7])

                result.map { it["number"] }.distinct()[0] shouldBe "9876543210"
            }
        }
    }

}