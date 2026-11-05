package com.example.duckdbpoc

import com.example.duckdbpoc.analysis.capacity.CapacityAnalyses
import com.example.duckdbpoc.analysis.capacity.CapacityParameters
import com.example.duckdbpoc.application.PipelineService
import com.example.duckdbpoc.framework.pipeline.AnalysisRequest
import com.example.duckdbpoc.framework.pipeline.InputFile
import com.example.duckdbpoc.framework.pipeline.InputFormat
import com.example.duckdbpoc.generated.memory.tables.references.*
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.duckdb.DuckDBConnection
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Actual production CapacityAnalysis Bean and execution path; local CSV fixtures only. */
@SpringBootTest
class CapacityAnalysisIntegrationTest {
    @Autowired lateinit var service: PipelineService
    @Autowired lateinit var anchor: DuckDBConnection
    @TempDir lateinit var directory: Path

    private val parameters = CapacityParameters(emptyList(), emptyList(), emptyList(), 0.25, false, false)

    @Test
    fun `aggregates quantity and preserves all output columns and reservation branches`() {
        val fixture = CapacityFixture(
            listOf(CapacityRow("A", 200, 100, 40), CapacityRow("B", 200, 100, 40),
                CapacityRow("C", 200, 100, 40), CapacityRow("D", 200, 100, 40),
                CapacityRow("E", 100, 80, 5), CapacityRow("NULL", null, null, null), CapacityRow("NO_PO")),
            listOf(OrderRow("A", 10, 2), OrderRow("A", 10, 3), OrderRow("B", 130, 20),
                OrderRow("C", 130, 50), OrderRow("D", 100, 20), OrderRow("E", 150, 20),
                OrderRow("A", 999, 999, type = "B"), OrderRow("A", 999, 999, date = "2026-11-06")),
        )
        val rows = execute(fixture)
        val a = rows.single { it[1] == "A" }
        assertThat(a).containsExactly(20261105, "A", "managed", "A", 180, true, 1.5,
            80, 40, 2.5, "hard", 200, 200, 100, 40, 20, 5, 50, 0.25, false)
        assertThat(rows.single { it[1] == "B" }[8]).isEqualTo(20)
        assertThat(rows.single { it[1] == "C" }[8]).isEqualTo(10)
        assertThat(rows.single { it[1] == "D" }[8]).isEqualTo(40)
        assertThat(rows.single { it[1] == "E" }[8]).isEqualTo(0)
    }

    @Test
    fun `missing or empty optional file uses RC XDock and PicoRC fallback per request`() {
        val fixture = CapacityFixture(listOf(CapacityRow("RC"), CapacityRow("PICO"), CapacityRow("X"), CapacityRow("OTHER")))
        val p = parameters.copy(rcList = listOf("RC", "PICO"), xDockList = listOf("X"), picoRcList = listOf("PICO"))
        execute(fixture, p, "missing")
        execute(fixture.copy(ratios = emptyList()), p, "empty")
        val enabled = execute(fixture, p.copy(useEarlyShufflingForPicoRC = true), "pico-enabled")
        assertThat(enabled.single { it[1] == "PICO" }[18]).isEqualTo(0.25)
        assertThat(enabled.single { it[1] == "RC" }[18]).isEqualTo(0.0)
    }

    @Test
    fun `specific then wildcard then fallback with first duplicate including null and whitespace`() {
        val fixture = CapacityFixture(
            listOf(CapacityRow("A"), CapacityRow("B"), CapacityRow("C"), CapacityRow("D"),
                CapacityRow("E"), CapacityRow("F"), CapacityRow(null, type = null)),
            orders = listOf(OrderRow(null, 10, 2, type = null)),
            ratios = listOf(RatioRow("A", "A", 0.6), RatioRow("A", "A", 0.9), RatioRow("A", null, 0.8),
                RatioRow("B", "A", null), RatioRow("B", "A", 0.9), RatioRow("B", "", 0.3),
                RatioRow("C", "A", 0.0), RatioRow("C", "", 0.7),
                RatioRow("D", "\t\u00a0", 0.4), RatioRow("D", "", 0.8),
                RatioRow("E", " ", null), RatioRow("E", null, 0.9),
                RatioRow("F", "B", 0.9), RatioRow(null, "", 0.2)),
        )
        execute(fixture, parameters.copy(rcList = listOf("A")))
    }

    @Test
    fun `shutdown overrides unlimited capacity and duplicate shutdown rows remain duplicate rows`() {
        val fixture = CapacityFixture(listOf(CapacityRow("CLOSED"), CapacityRow("OPEN")),
            listOf(OrderRow("CLOSED", 30, 5)),
            listOf(ShutdownRow("CLOSED"), ShutdownRow("CLOSED"), ShutdownRow("OPEN", "2026-11-06")),
            listOf(RatioRow("OPEN", "A", 0.9)))
        execute(fixture)
        val rows = execute(fixture, parameters.copy(isUnLimitCapacity = true), "unlimited")
        assertThat(rows.filter { it[1] == "CLOSED" }).hasSize(2)
        assertThat(rows.single { it[1] == "OPEN" }[4]).isEqualTo(Int.MAX_VALUE)
    }

    @Test
    fun `Kotlin truncation saturation NaN negative reservation and Int overflow are preserved`() {
        val fixture = CapacityFixture(
            listOf("FRACTION", "NEGATIVE", "INFINITY", "NAN", "MIN", "OVERFLOW", "SUBTRACT").map {
                CapacityRow(it, if (it == "SUBTRACT") Int.MIN_VALUE else 10, 100, 20)
            },
            listOf(OrderRow("OVERFLOW", Int.MAX_VALUE, 0), OrderRow("OVERFLOW", 1, 0), OrderRow("SUBTRACT", 1, 0)),
            ratios = listOf(RatioRow("FRACTION", "A", 0.29), RatioRow("NEGATIVE", "A", -0.29),
                RatioRow("INFINITY", "A", Double.POSITIVE_INFINITY), RatioRow("NAN", "A", Double.NaN),
                RatioRow("MIN", "A", Double.NEGATIVE_INFINITY)),
        )
        val rows = execute(fixture)
        assertThat(rows.single { it[1] == "FRACTION" }[17]).isEqualTo(2)
        assertThat(rows.single { it[1] == "NEGATIVE" }[17]).isEqualTo(-2)
        assertThat(rows.single { it[1] == "OVERFLOW" }[15]).isEqualTo(Int.MIN_VALUE)
    }

    @Test
    fun `empty capacity produces header only and selected optional file failures propagate`() {
        execute(CapacityFixture(emptyList()))
        val fixture = CapacityFixture(listOf(CapacityRow("A")))
        val inputs = fixture.write(directory.resolve("failure"))
        val request = AnalysisRequest(CapacityAnalyses.CAPACITY,
            inputs + (FC_EARLY_SHUFFLING_CONFIG to InputFile(directory.resolve("missing.csv").toString(), InputFormat.CSV)),
            directory.resolve("failure.csv").toString(), parameters)
        assertThatThrownBy { service.process(request) }.isInstanceOf(org.jooq.exception.DataAccessException::class.java)
        assertThat(Files.exists(Path.of(request.output))).isFalse()
        assertThatThrownBy { service.process(request.copy(inputs = inputs - PURCHASE_ORDER)) }
            .isInstanceOf(NoSuchElementException::class.java)
        assertCleanedUp()
    }

    @Test
    fun `Parquet and CSV inputs can be mixed without changing duplicate config selection`() {
        val fixture = CapacityFixture(listOf(CapacityRow("A")), listOf(OrderRow("A", 20, 5)),
            ratios = listOf(RatioRow("A", "A", 0.3), RatioRow("A", "A", 0.9)))
        val inputs = fixture.write(directory.resolve("mixed")).toMutableMap()
        for (table in listOf(PURCHASE_ORDER, FC_EARLY_SHUFFLING_CONFIG)) {
            val parquet = directory.resolve("${table.name}.parquet")
            anchor.duplicate().use { connection ->
                connection.prepareStatement("COPY (SELECT * FROM read_csv(?, header=true)) TO ? (FORMAT PARQUET)").use {
                    it.setString(1, inputs.getValue(table).address)
                    it.setString(2, parquet.toString())
                    it.execute()
                }
            }
            inputs[table] = InputFile(parquet.toString(), InputFormat.PARQUET)
        }
        val output = directory.resolve("mixed.csv")
        service.process(AnalysisRequest(CapacityAnalyses.CAPACITY, inputs, output.toString(), parameters))
        anchor.duplicate().use {
            assertThat(readCapacityCsv(it, output)).containsExactlyInAnyOrderElementsOf(fixture.expected(parameters))
        }
        assertCleanedUp()
    }

    @Test
    fun `null order quantities fail instead of being silently skipped by SQL SUM`() {
        for (column in listOf(PURCHASE_ORDER.QUANTITY, PURCHASE_ORDER.MODELALLOCATEDQUANTITY)) {
            val inputs = CapacityFixture(listOf(CapacityRow("A"))).write(directory.resolve(column.name))
            val values = linkedMapOf("date" to "2026-11-05", "sku" to "1", "fc" to "A", "type" to "A",
                "quantity" to "10", "confirmedQuantity" to "123", "modelAllocatedQuantity" to "2")
            values[column.name] = ""
            Files.writeString(Path.of(inputs.getValue(PURCHASE_ORDER).address),
                values.keys.joinToString(",") + "\n" + values.values.joinToString(",") + "\n")
            val output = directory.resolve("null-${column.name}.csv")
            assertThatThrownBy { service.process(AnalysisRequest(CapacityAnalyses.CAPACITY, inputs, output.toString(), parameters)) }
                .isInstanceOf(org.jooq.exception.DataAccessException::class.java)
            assertThat(Files.exists(output)).isFalse()
            assertCleanedUp()
        }
    }

    @Test
    fun `one hundred capacity requests share files but isolate parameters and results`() {
        val fixture = CapacityFixture(listOf(CapacityRow("RC"), CapacityRow("PICO"), CapacityRow("X")),
            orders = listOf(OrderRow("RC", 30, 5)))
        val inputs = fixture.write(directory.resolve("concurrent"))
        val ready = CountDownLatch(100)
        val start = CountDownLatch(1)
        val workers = Executors.newFixedThreadPool(100)
        try {
            val futures = (1..100).map { job -> workers.submit {
                ready.countDown()
                check(start.await(30, TimeUnit.SECONDS))
                val p = parameters.copy(rcList = listOf("RC", "PICO"), xDockList = listOf("X"), picoRcList = listOf("PICO"),
                    defaultEarlyShufflingRatio = job / 100.0, useEarlyShufflingForPicoRC = job % 2 == 0,
                    isUnLimitCapacity = job % 3 == 0)
                val output = directory.resolve("job-$job.csv")
                service.process(AnalysisRequest(CapacityAnalyses.CAPACITY, inputs, output.toString(), p))
                anchor.duplicate().use { connection ->
                    assertThat(readCapacityCsv(connection, output)).containsExactlyInAnyOrderElementsOf(fixture.expected(p))
                }
            } }
            check(ready.await(30, TimeUnit.SECONDS))
            start.countDown()
            futures.forEach { it.get(120, TimeUnit.SECONDS) }
        } finally {
            start.countDown()
            workers.shutdown()
            if (!workers.awaitTermination(120, TimeUnit.SECONDS)) workers.shutdownNow()
        }
        assertCleanedUp()
    }

    private fun execute(fixture: CapacityFixture, p: CapacityParameters = parameters, name: String = "run"): List<List<Any?>> {
        val inputs = fixture.write(directory.resolve(name))
        val output = directory.resolve("$name.csv")
        val result = service.process(AnalysisRequest(CapacityAnalyses.CAPACITY, inputs, output.toString(), p))
        assertThat(result.output).isEqualTo(output.toString())
        assertThat(Files.readAllLines(output).first()).isEqualTo(
            "date,center_code,center_management_type,capacity_type,capacity,overflow_not_allowed,hardcap,revised_capacity," +
                "reserved_capacity,backlog_version,hardcap_type,old_capacity,original_capacity,original_revised_capacity," +
                "original_reserved_capacity,open_order,model_allocated,early_shuffling_reservation,early_shuffling_ratio,blocked_fc",
        )
        val rows = anchor.duplicate().use { readCapacityCsv(it, output) }
        assertThat(rows).containsExactlyInAnyOrderElementsOf(fixture.expected(p))
        assertCleanedUp()
        return rows
    }

    private fun assertCleanedUp() {
        anchor.duplicate().use { connection -> connection.createStatement().use { statement ->
            statement.executeQuery("SELECT count(*) FROM information_schema.schemata WHERE schema_name LIKE 'job_%'").use {
                it.next(); assertThat(it.getInt(1)).isZero()
            }
            statement.executeQuery("SELECT count FROM duckdb_connection_count()").use {
                it.next(); assertThat(it.getInt(1)).isEqualTo(2)
            }
        } }
    }
}
