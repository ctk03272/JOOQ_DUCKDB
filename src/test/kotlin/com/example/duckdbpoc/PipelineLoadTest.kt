package com.example.duckdbpoc

import com.example.duckdbpoc.framework.pipeline.PipelineResult
import com.example.duckdbpoc.framework.analysis.AnalysisRegistry
import com.example.duckdbpoc.framework.pipeline.PipelineExecutor

import org.assertj.core.api.Assertions.assertThat
import org.duckdb.DuckDBConnection
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Explicitly run through Gradle loadTest; never included in the application JAR. */
@Tag("load")
@SpringBootTest
class PipelineLoadTest {
    @Autowired lateinit var pipelineExecutor: PipelineExecutor
    @Autowired lateinit var anchor: DuckDBConnection
    @Autowired lateinit var registry: AnalysisRegistry

    @Test
    fun `process the same input concurrently with isolated results`() {
        val count = System.getProperty("load.jobs", "100").toInt()
        val directory = Path.of("build/load-test-results", "run-${UUID.randomUUID()}")
        Files.createDirectories(directory)
        val ready = CountDownLatch(count)
        val start = CountDownLatch(1)
        val active = AtomicInteger()
        val peak = AtomicInteger()
        val executor = Executors.newFixedThreadPool(count)
        try {
            val futures = (1..count).map { job ->
                executor.submit<Pair<Int, Result<PipelineResult>>> {
                    ready.countDown()
                    check(start.await(60, TimeUnit.SECONDS)) { "Start barrier timed out" }
                    job to runCatching {
                        val current = active.incrementAndGet()
                        peak.accumulateAndGet(current, ::maxOf)
                        try {
                            executeFixture(registry, pipelineExecutor, if (job % 2 == 0) "monthly-usage" else "monthly-price", Path.of("excmples"), directory.resolve("job-$job/processing.csv"))
                        } finally {
                            active.decrementAndGet()
                        }
                    }
                }
            }
            check(ready.await(60, TimeUnit.SECONDS)) { "Workers did not reach start barrier" }
            start.countDown()
            val outcomes = futures.map { it.get(120, TimeUnit.SECONDS) }
            val successes = outcomes.mapNotNull { it.second.getOrNull() }
            val groups = outcomes.filter { it.second.isSuccess }.groupBy { it.first % 2 }
            val identical = groups.isNotEmpty() && groups.values.all { group ->
                val reference = csvContents(group.first().second.getOrThrow().output.localPath)
                group.all { csvContents(it.second.getOrThrow().output.localPath) == reference }
            }
            outcomes.forEach { (_, outcome) -> outcome.getOrThrow() }
            assertThat(successes).hasSize(count)
            assertThat(identical).isTrue()
            assertThat(successes.map { it.output }.toSet()).hasSize(count)
            assertThat(peak.get()).isGreaterThan(1)
        } finally {
            start.countDown()
            executor.shutdown()
            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                executor.shutdownNow()
                check(executor.awaitTermination(30, TimeUnit.SECONDS)) { "Workers did not terminate" }
            }
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
}
