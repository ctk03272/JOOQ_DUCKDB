package com.example.duckdbpoc

import com.example.duckdbpoc.analysis.capacity.CapacityParameters
import com.example.duckdbpoc.generated.memory.tables.references.CAPACITY
import com.example.duckdbpoc.generated.memory.tables.references.FC_SHUTDOWN
import com.example.duckdbpoc.generated.memory.tables.references.PURCHASE_ORDER
import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.duckdb.DuckDBConnection
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path

/** Real HTTP server and production Beans/settings; local input/output fixtures, not S3 IO proof. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CapacityControllerIntegrationTest {
    @LocalServerPort private var port: Int = 0
    @Autowired lateinit var mapper: ObjectMapper
    @Autowired lateinit var anchor: DuckDBConnection
    @TempDir lateinit var directory: Path

    @Test
    fun `HTTP request maps input roles and per-call parameters to the configured analysis`() {
        val fixture = CapacityFixture(
            listOf(CapacityRow("PICO"), CapacityRow("RC"), CapacityRow("X"), CapacityRow("OTHER")),
            listOf(OrderRow("OTHER", 10, 2)),
        )
        val inputs = fixture.write(directory.resolve("inputs"))
        val parameters = listOf(
            CapacityParameters(listOf("RC", "PICO"), listOf("X"), listOf("PICO"), 0.37, false, true),
            CapacityParameters(emptyList(), emptyList(), emptyList(), 0.5, true, false),
        )
        HttpClient.newHttpClient().use { client ->
            parameters.forEachIndexed { index, p ->
                val output = directory.resolve("result-$index.csv")
                // Use literal API field names to check Kotlin Boolean JSON binding as well.
                val body = mapper.writeValueAsString(mapOf(
                    "capacityUri" to inputs.getValue(CAPACITY).address,
                    "purchaseOrderUri" to inputs.getValue(PURCHASE_ORDER).address,
                    "shutdownUri" to inputs.getValue(FC_SHUTDOWN).address,
                    "outputUri" to output.toString(),
                    "rcList" to p.rcList,
                    "xDockList" to p.xDockList,
                    "picoRcList" to p.picoRcList,
                    "defaultEarlyShufflingRatio" to p.defaultEarlyShufflingRatio,
                    "isUnLimitCapacity" to p.isUnLimitCapacity,
                    "useEarlyShufflingForPicoRC" to p.useEarlyShufflingForPicoRC,
                ))
                val response = client.send(
                    HttpRequest.newBuilder(URI("http://localhost:$port/analyses/capacity"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString(),
                )
                assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(200)
                assertThat(mapper.readTree(response.body())).isEqualTo(
                    mapper.valueToTree(mapOf("output" to output.toString())),
                )
                anchor.duplicate().use { connection ->
                    assertThat(readCapacityCsv(connection, output))
                        .containsExactlyInAnyOrderElementsOf(fixture.expected(p))
                }
            }
        }
    }
}
