package com.example.duckdbpoc

import com.example.duckdbpoc.analysis.monthly.MonthlyAnalyses
import com.example.duckdbpoc.framework.analysis.AnalysisId

import com.example.duckdbpoc.application.PipelineService
import com.example.duckdbpoc.framework.pipeline.PipelineExecutor
import com.example.duckdbpoc.framework.pipeline.InputFile
import com.example.duckdbpoc.framework.pipeline.InputFormat
import com.example.duckdbpoc.framework.pipeline.AnalysisRequest
import com.example.duckdbpoc.adapter.input.ParquetLoader
import com.example.duckdbpoc.framework.analysis.AnalysisRegistry
import com.example.duckdbpoc.infrastructure.session.JobDatabase
import com.example.duckdbpoc.generated.memory.tables.references.READINGS
import java.math.BigDecimal
import com.example.duckdbpoc.framework.pipeline.PipelineResult

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import org.duckdb.DuckDBConnection
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@SpringBootTest
class PipelineIntegrationTest {
    @Autowired lateinit var executor: PipelineExecutor
    @Autowired lateinit var pipeline: PipelineService
    @Autowired lateinit var jobs: JobDatabase
    @Autowired lateinit var registry: AnalysisRegistry
    @Autowired lateinit var csvLoader: com.example.duckdbpoc.adapter.input.CsvLoader
    @Autowired lateinit var loader: ParquetLoader
    @Autowired lateinit var anchor: DuckDBConnection
    @TempDir lateinit var directory: Path

    @Test
    fun `usage analysis exports monthly statistics without price columns`() {
        fixture()
        val output = directory.resolve("report 'quoted'/result.csv")
        processFixture(MonthlyAnalyses.USAGE, directory, output)
        assertThat(csvContents(output).first).isEqualTo(
            "system_id,system_name,reading_year,reading_month,reading_count,power_sum,power_average",
        )
        assertThat(csvContents(output).second).containsExactlyInAnyOrder(
            "1,\"Alpha, East\",2024,1,1,10.000,10.000000",
            "1,\"Alpha, East\",2024,2,2,6.000,3.000000",
            "2,Beta,2024,1,1,5.000,5.000000",
        )
        val firstOutput = csvContents(output)
        processFixture(MonthlyAnalyses.USAGE, directory, output)
        assertThat(csvContents(output)).isEqualTo(firstOutput)
    }

    @Test
    fun `processes the supplied parquet files`() {
        val output = directory.resolve("sample.csv")
        processFixture(MonthlyAnalyses.USAGE, Path.of("excmples"), output)
        assertThat(Files.size(output)).isPositive()
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.prepareStatement("SELECT sum(reading_count) FROM read_csv(?, header=true)").use { query ->
                query.setString(1, output.toString())
                query.executeQuery().use { rows ->
                    assertThat(rows.next()).isTrue()
                    assertThat(rows.getLong(1)).isEqualTo(151879L)
                }
            }
            connection.createStatement().use { query ->
                query.executeQuery("SELECT count(*) FROM information_schema.tables WHERE table_schema='pipeline'").use { rows ->
                    rows.next()
                    assertThat(rows.getInt(1)).isZero()
                }
            }
        }
    }

    @Test
    fun `one hundred concurrent jobs produce independent identical outputs and release schemas and connections`() {
        fixture()
        val ready = CountDownLatch(100)
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(100)
        try {
            val futures = (1..100).map { job ->
                executor.submit<PipelineResult> {
                    ready.countDown()
                    check(start.await(30, TimeUnit.SECONDS))
                    processFixture(MonthlyAnalyses.USAGE, directory, directory.resolve("job-$job.csv"))
                }
            }
            check(ready.await(30, TimeUnit.SECONDS))
            start.countDown()
            val results = futures.map { it.get(90, TimeUnit.SECONDS) }
            assertThat(results.map { it.output }.toSet()).hasSize(100)
            assertThat(results.map { csvContents(it.output.localPath) }.toSet()).hasSize(1)
            assertThat(Files.readAllLines(results.first().output.localPath)).hasSize(4)
        } finally {
            start.countDown()
            executor.shutdown()
            if (!executor.awaitTermination(90, TimeUnit.SECONDS)) executor.shutdownNow()
        }
        assertCleanedUp()
    }

    @Test
    fun `usage configuration does not load prices and preserves timestamp precision`() {
        fixture()
        Files.delete(directory.resolve("prices.parquet"))
        val output = directory.resolve("usage.csv")
        processFixture(MonthlyAnalyses.USAGE, directory, output)
        assertThat(Files.readAllLines(output)).hasSize(4)
        val definition = registry.get(MonthlyAnalyses.USAGE)
        jobs.use(definition.schema, definition.requiredInputs + definition.outputTable) { ctx ->
            definition.requiredInputs.forEach { loader.load(ctx, it, directory.resolve("${it.name}.parquet").toString()) }
            val readingTime = ctx.select(READINGS.READ_ON).from(READINGS)
                .where(READINGS.POWER.eq(BigDecimal("10.000"))).fetchSingle().value1()
            assertThat(readingTime!!.nano).isEqualTo(123456000)
        }
        assertCleanedUp()
    }

    @Test
    fun `price configuration runs independently without systems and exports only prices`() {
        fixture()
        Files.delete(directory.resolve("systems.parquet"))
        val output = directory.resolve("price.csv")
        processFixture(MonthlyAnalyses.PRICE, directory, output)
        assertThat(csvContents(output).first).isEqualTo(
            "system_id,reading_year,reading_month,priced_power_sum",
        )
        assertThat(csvContents(output).second).containsExactlyInAnyOrder(
            "1,2024,1,20.00000",
            "1,2024,2,18.00000",
            "2,2024,1,10.00000",
        )
        assertCleanedUp()
    }

    @Test
    fun `unknown analysis fails without writing output or leaking a job`() {
        val output = directory.resolve("unknown.csv")
        assertThatThrownBy { processFixture(AnalysisId<Unit>("unknown"), directory, output) }
            .isInstanceOf(NoSuchElementException::class.java)
        assertThat(Files.exists(output)).isFalse()
        assertCleanedUp()
    }

    @Test
    fun `request accepts arbitrary input filenames and rejects missing table mapping`() {
        fixture()
        val systems = directory.resolve("request-systems-123.parquet")
        val readings = directory.resolve("request-readings-789.parquet")
        Files.move(directory.resolve("systems.parquet"), systems)
        Files.move(directory.resolve("readings.parquet"), readings)
        val service = PipelineService(executor, registry)
        val request = AnalysisRequest(MonthlyAnalyses.USAGE, mapOf(
            com.example.duckdbpoc.generated.memory.tables.references.SYSTEMS to InputFile(systems.toString(), InputFormat.PARQUET),
            READINGS to InputFile(readings.toString(), InputFormat.PARQUET),
        ), output = directory.resolve("request.csv").toString(), parameters = Unit)
        val result = service.process(request)
        assertThat(Files.readAllLines(result.output.localPath)).hasSize(4)
        assertThatThrownBy { service.process(request.copy(inputs = mapOf(READINGS to InputFile(readings.toString(), InputFormat.PARQUET)))) }
            .hasStackTraceContaining("NoSuchElementException")
        assertCleanedUp()
    }

    @Test
    fun `DuckDB rejects nonexistent input and job resources are cleaned up`() {
        fixture()
        Files.delete(directory.resolve("readings.parquet"))
        val output = directory.resolve("missing-input.csv")
        assertThatThrownBy { processFixture(MonthlyAnalyses.USAGE, directory, output) }
            .isInstanceOf(org.jooq.exception.DataAccessException::class.java)
            .hasRootCauseInstanceOf(java.sql.SQLException::class.java)
        assertThat(Files.exists(output)).isFalse()
        assertCleanedUp()
    }

    @Test
    fun `only selected tables are created even across domain files`() {
        for ((id, names) in mapOf(
            MonthlyAnalyses.USAGE to listOf("systems", "readings", "monthly_usage"),
            MonthlyAnalyses.PRICE to listOf("prices", "readings", "monthly_price"),
        )) {
            val definition = registry.get(id)
            jobs.use(definition.schema, definition.requiredInputs + definition.outputTable) { ctx ->
                val tables = ctx.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE 'job_%'")
                    .map { it.get(0, String::class.java) }
                assertThat(tables).containsExactlyInAnyOrderElementsOf(names)
            }
            assertCleanedUp()
        }
    }

    @Test
    fun `CSV and Parquet mixed inputs produce the same CSV as Parquet inputs`() {
        fixture()
        val csv = directory.resolve("systems.csv")
        Files.writeString(csv, "name,id\n\"Alpha, East\",1\nBeta,2\n")
        val expected = directory.resolve("expected.csv")
        processFixture(MonthlyAnalyses.USAGE, directory, expected)
        val output = directory.resolve("mixed.csv")
        pipeline.process(AnalysisRequest(
            MonthlyAnalyses.USAGE,
            mapOf(
                com.example.duckdbpoc.generated.memory.tables.references.SYSTEMS to InputFile(csv.toString(), InputFormat.CSV),
                READINGS to InputFile(directory.resolve("readings.parquet").toString(), InputFormat.PARQUET),
            ),
            output.toString(),
            parameters = Unit,
        ))
        assertThat(csvContents(output)).isEqualTo(csvContents(expected))
        assertCleanedUp()
    }

    @Test
    fun `CSV numeric and timestamp values preserve the declared table types`() {
        fixture()
        val csv = directory.resolve("readings.csv")
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.prepareStatement("COPY (SELECT power, read_on, system_id FROM read_parquet(?)) TO ? (FORMAT CSV, HEADER true)").use { statement ->
                statement.setString(1, directory.resolve("readings.parquet").toString())
                statement.setString(2, csv.toString())
                statement.execute()
            }
        }
        val definition = registry.get(MonthlyAnalyses.PRICE)
        jobs.use(definition.schema, listOf(READINGS)) { ctx ->
            csvLoader.load(ctx, READINGS, csv.toString())
            val row = ctx.select(READINGS.READ_ON, READINGS.POWER).from(READINGS)
                .where(READINGS.POWER.eq(BigDecimal("10.000"))).fetchSingle()
            assertThat(row.value1()!!.nano).isEqualTo(123456000)
            assertThat(row.value2()).isEqualByComparingTo(BigDecimal("10.000"))
        }
        assertCleanedUp()
    }

    private fun processFixture(id: AnalysisId<Unit>, input: Path, output: Path) =
        executeFixture(registry, executor, id, input, output)

    private fun assertCleanedUp() {
        anchor.duplicate().use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT count(*) FROM information_schema.schemata WHERE schema_name LIKE 'job_%'").use {
                    it.next()
                    assertThat(it.getInt(1)).isZero()
                }
                statement.executeQuery("SELECT count FROM duckdb_connection_count()").use {
                    it.next()
                    assertThat(it.getInt(1)).isEqualTo(2)
                }
            }
        }
    }

    private fun fixture() {
        val priceRows = listOf(
            "(1, 2.00, DATE '2024-01-01', DATE '2024-02-01')",
            "(2, 3.00, DATE '2024-02-01', DATE '2024-03-01')",
        )
        val selects = mapOf(
            "systems" to "SELECT * FROM (VALUES (1, 'Alpha, East'), (2, 'Beta')) t(id, name)",
            "prices" to "SELECT * FROM (VALUES ${priceRows.joinToString()}) t(id, value, valid_from, valid_until)",
            "readings" to """
                SELECT * FROM (VALUES
                    (1, TIMESTAMP '2024-01-31 23:45:00.123456', 10.000),
                    (1, TIMESTAMP '2024-02-01 00:00:00', 4.000),
                    (1, TIMESTAMP '2024-02-01 00:15:00', 2.000),
                    (2, TIMESTAMP '2024-01-15 12:00:00', 5.000)
                ) t(system_id, read_on, power)
            """.trimIndent(),
        )
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            for ((name, select) in selects) {
                val path = directory.resolve("$name.parquet")
                Files.deleteIfExists(path)
                connection.prepareStatement("COPY ($select) TO ? (FORMAT PARQUET)").use { copy ->
                    copy.setString(1, path.toString())
                    copy.execute()
                }
            }
        }
    }
}
