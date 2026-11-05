package com.example.duckdbpoc

import com.example.duckdbpoc.analysis.monthly.MonthlyAnalyses

import com.example.duckdbpoc.application.PipelineService
import com.example.duckdbpoc.configuration.AwsS3Settings
import com.example.duckdbpoc.framework.pipeline.InputFile
import com.example.duckdbpoc.framework.pipeline.InputFormat
import com.example.duckdbpoc.framework.pipeline.AnalysisRequest
import com.example.duckdbpoc.generated.memory.tables.references.PRICES
import com.example.duckdbpoc.generated.memory.tables.references.READINGS
import com.example.duckdbpoc.generated.memory.tables.references.SYSTEMS
import org.assertj.core.api.Assertions.assertThat
import org.duckdb.DuckDBConnection
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

/** Uses production beans. The caller supplies input and output addresses through requests. */
@Tag("s3")
@SpringBootTest
class S3IntegrationTest {
    @Autowired private lateinit var service: PipelineService
    @Autowired private lateinit var anchor: DuckDBConnection
    @Autowired private lateinit var aws: AwsS3Settings

    @Test
    fun `monthly price uses actual Spring configuration for S3 input and output`() {
        val request = AnalysisRequest(MonthlyAnalyses.PRICE, mapOf(
            PRICES to InputFile(System.getenv().getValue("S3_TEST_PRICES_URI"), InputFormat.PARQUET),
            READINGS to InputFile(System.getenv().getValue("S3_TEST_READINGS_URI"), InputFormat.PARQUET),
        ), output = System.getenv().getValue("S3_TEST_PRICE_OUTPUT_URI"), parameters = Unit)
        verify(request)
        verify(request.copy(output = System.getenv().getValue("S3_TEST_PRICE_ALTERNATE_OUTPUT_URI")))
    }

    @Test
    fun `monthly usage uses actual Spring configuration for S3 input and output`() {
        verify(AnalysisRequest(MonthlyAnalyses.USAGE, mapOf(
            SYSTEMS to InputFile(System.getenv().getValue("S3_TEST_SYSTEMS_URI"), InputFormat.PARQUET),
            READINGS to InputFile(System.getenv().getValue("S3_TEST_READINGS_URI"), InputFormat.PARQUET),
        ), output = System.getenv().getValue("S3_TEST_USAGE_OUTPUT_URI"), parameters = Unit))
    }

    @Test
    fun `real S3 CSV input is loaded through the actual configured service`() {
        val csvAddress = System.getenv().getValue("S3_TEST_READINGS_CSV_URI")
        // This is test fixture preparation; both addresses are explicitly provided by the caller.
        anchor.duplicate().use { connection ->
            connection.prepareStatement("COPY (SELECT * FROM read_parquet(?)) TO ? (FORMAT CSV, HEADER true)").use { statement ->
                statement.setString(1, System.getenv().getValue("S3_TEST_READINGS_URI"))
                statement.setString(2, csvAddress)
                statement.execute()
            }
        }
        verify(AnalysisRequest(MonthlyAnalyses.PRICE, mapOf(
            PRICES to InputFile(System.getenv().getValue("S3_TEST_PRICES_URI"), InputFormat.PARQUET),
            READINGS to InputFile(csvAddress, InputFormat.CSV),
        ), output = System.getenv().getValue("S3_TEST_CSV_OUTPUT_URI"), parameters = Unit))
    }

    private fun verify(request: AnalysisRequest<Unit>) {
        assertThat(aws.enabled).isTrue()
        assertThat(aws.region).isEqualTo("ap-northeast-2")
        assertThat(aws.profile).isEqualTo("duckdb-poc")

        val result = service.process(request)
        assertThat(result.output).isEqualTo(request.output)
        anchor.duplicate().use { connection ->
            connection.prepareStatement("SELECT count(*) FROM read_csv(?, header=true)").use { statement ->
                statement.setString(1, result.output)
                statement.executeQuery().use { rows ->
                    assertThat(rows.next()).isTrue()
                    assertThat(rows.getLong(1)).isPositive()
                }
            }
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT count(*) FROM information_schema.schemata WHERE schema_name LIKE 'job_%'").use { rows ->
                    assertThat(rows.next()).isTrue()
                    assertThat(rows.getLong(1)).isZero()
                }
            }
        }
    }
}
