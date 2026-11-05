package com.example.duckdbpoc

import com.example.duckdbpoc.application.PipelineService
import com.example.duckdbpoc.framework.analysis.AnalysisId
import com.example.duckdbpoc.framework.analysis.ConfiguredAnalysis
import com.example.duckdbpoc.framework.analysis.DuckDbAnalysis
import com.example.duckdbpoc.framework.pipeline.AnalysisRequest
import com.example.duckdbpoc.framework.pipeline.InputFile
import com.example.duckdbpoc.framework.pipeline.InputFormat
import com.example.duckdbpoc.generated.memory.Pipeline
import com.example.duckdbpoc.generated.memory.tables.references.MONTHLY_PRICE
import com.example.duckdbpoc.generated.memory.tables.references.READINGS
import com.example.duckdbpoc.generated.memory.tables.references.SYSTEMS
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.duckdb.DuckDBConnection
import org.jooq.impl.DSL.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Adds test analyses; keeps the application's service, registry, executor and DB/AWS settings. */
@SpringBootTest
@Import(AnalysisParametersIntegrationTest.Analyses::class)
class AnalysisParametersIntegrationTest {
    @Autowired lateinit var service: PipelineService
    @Autowired lateinit var anchor: DuckDBConnection
    @TempDir lateinit var directory: Path

    data class Parameters(val systemIds: List<Int>, val multiplier: BigDecimal, val enabled: Boolean)

    companion object {
        val SCALED = AnalysisId<Parameters>("test-scaled")
        val NAMED = AnalysisId<String>("test-named")
    }

    class ScaledAnalysis : DuckDbAnalysis<Parameters> {
        override fun query(parameters: Parameters) = select(
            READINGS.SYSTEM_ID.`as`(MONTHLY_PRICE.SYSTEM_ID),
            year(READINGS.READ_ON).`as`(MONTHLY_PRICE.READING_YEAR),
            month(READINGS.READ_ON).`as`(MONTHLY_PRICE.READING_MONTH),
            sum(READINGS.POWER).mul(parameters.multiplier).`as`(MONTHLY_PRICE.PRICED_POWER_SUM),
        ).from(READINGS)
            .where(if (parameters.enabled) READINGS.SYSTEM_ID.`in`(parameters.systemIds) else falseCondition())
            .groupBy(READINGS.SYSTEM_ID, year(READINGS.READ_ON), month(READINGS.READ_ON))
    }

    @TestConfiguration(proxyBeanMethods = false)
    class Analyses {
        @Bean
        fun scaled() = ConfiguredAnalysis(
            SCALED, Pipeline.PIPELINE, ScaledAnalysis(), listOf(READINGS), MONTHLY_PRICE,
        )

        @Bean
        fun named() = ConfiguredAnalysis(
            NAMED, Pipeline.PIPELINE,
            object : DuckDbAnalysis<String> {
                override fun query(parameters: String) = select(
                    `val`(1).`as`(SYSTEMS.ID), `val`(parameters).`as`(SYSTEMS.NAME),
                )
            },
            emptyList(), SYSTEMS,
        )
    }

    @Test
    fun `one hundred calls use their own list number and flag through the injected service`() {
        val input = directory.resolve("readings.csv")
        Files.writeString(input, "system_id,read_on,power\n1,2024-01-01 00:00:00,10\n2,2024-01-01 00:00:00,20\n")
        val ready = CountDownLatch(100)
        val start = CountDownLatch(1)
        val workers = Executors.newFixedThreadPool(100)
        try {
            val futures = (1..100).map { job ->
                workers.submit {
                    ready.countDown()
                    check(start.await(30, TimeUnit.SECONDS))
                    val systemId = job % 2 + 1
                    val enabled = job % 3 != 0
                    val result = service.process(AnalysisRequest(
                        analysisId = SCALED,
                        inputs = mapOf(READINGS to InputFile(input.toString(), InputFormat.CSV)),
                        output = directory.resolve("scaled-$job.csv").toString(),
                        parameters = Parameters(listOf(systemId), BigDecimal(job), enabled),
                    ))
                    val lines = Files.readAllLines(Path.of(result.output))
                    assertThat(lines.first()).isEqualTo("system_id,reading_year,reading_month,priced_power_sum")
                    if (enabled) {
                        assertThat(lines.drop(1)).containsExactly("$systemId,2024,1,${systemId * 10 * job}.00000")
                    } else {
                        assertThat(lines).hasSize(1)
                    }
                }
            }
            check(ready.await(30, TimeUnit.SECONDS))
            start.countDown()
            futures.forEach { it.get(90, TimeUnit.SECONDS) }
        } finally {
            start.countDown()
            workers.shutdown()
            if (!workers.awaitTermination(90, TimeUnit.SECONDS)) workers.shutdownNow()
        }
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

    @Test
    fun `registry supports another parameter type and does not confuse equal display names`() {
        val request = AnalysisRequest(
            NAMED, emptyMap(), directory.resolve("named.csv").toString(), parameters = "First",
        )
        service.process(request)
        assertThat(Files.readAllLines(Path.of(request.output))).containsExactly("id,name", "1,First")
        val second = request.copy(parameters = "Second", output = directory.resolve("second.csv").toString())
        service.process(second)
        assertThat(Files.readAllLines(Path.of(second.output))).containsExactly("id,name", "1,Second")

        val unregistered = AnalysisRequest(
            AnalysisId<Int>(NAMED.value), emptyMap(), directory.resolve("unknown.csv").toString(), parameters = 123,
        )
        assertThatThrownBy { service.process(unregistered) }.isInstanceOf(NoSuchElementException::class.java)
        assertThat(Files.exists(Path.of(unregistered.output))).isFalse()
    }
}
