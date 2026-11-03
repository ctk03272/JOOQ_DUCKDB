package com.example.duckdbpoc

import com.example.duckdbpoc.adapter.input.ParquetLoader
import com.example.duckdbpoc.adapter.output.CsvExporter
import com.example.duckdbpoc.configuration.AwsS3Settings
import com.example.duckdbpoc.generated.memory.tables.references.PRICES
import com.example.duckdbpoc.infrastructure.aws.DuckDbS3Initializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.tools.jdbc.MockConnection
import org.jooq.tools.jdbc.MockDataProvider
import org.jooq.tools.jdbc.MockResult
import org.junit.jupiter.api.Test
import java.sql.SQLException

/** SQL adapter contract tests; these do not claim successful access to real AWS S3. */
class S3AdapterTest {
    @Test
    fun `S3 read passes the request address as a bound parameter without local file access`() {
        val address = "s3://example-bucket/input/price'2026.parquet"
        val connection = MockConnection(MockDataProvider { call ->
            assertThat(call.sql()).contains("read_parquet")
            assertThat(call.sql()).doesNotContain(address)
            assertThat(call.bindings().toList()).contains(address)
            arrayOf(MockResult(1))
        })
        connection.use {
            ParquetLoader().load(DSL.using(it, SQLDialect.DUCKDB), PRICES, address)
        }
    }

    @Test
    fun `CSV read binds source address and disables inferred input types`() {
        val address = "s3://example-bucket/input/price'2026.csv"
        MockConnection(MockDataProvider { call ->
            assertThat(call.sql()).contains("read_csv", "all_varchar = true")
            assertThat(call.sql()).doesNotContain(address)
            assertThat(call.bindings().toList()).contains(address)
            arrayOf(MockResult(1))
        }).use {
            com.example.duckdbpoc.adapter.input.CsvLoader().load(DSL.using(it, SQLDialect.DUCKDB), PRICES, address)
        }
    }

    @Test
    fun `S3 export binds COPY destination directly and propagates remote failure`() {
        val address = "s3://example-bucket/output/result'2026.csv"
        val connection = MockConnection(MockDataProvider { call ->
            assertThat(call.sql()).contains("COPY (").contains("TO ?")
            assertThat(call.sql()).doesNotContain(address)
            assertThat(call.bindings().toList()).contains(address)
            throw SQLException("remote upload failed")
        })
        connection.use {
            assertThatThrownBy {
                CsvExporter().export(DSL.using(it, SQLDialect.DUCKDB), DSL.selectOne(), address)
            }.hasStackTraceContaining("remote upload failed")
        }
    }

    @Test
    fun `credential configuration uses an ephemeral credential chain secret with escaped metadata`() {
        val sql = DuckDbS3Initializer.secretSql(AwsS3Settings(
            enabled = true, region = "ap-northeast-2", profile = "dev'profile", credentialChain = "config",
        ))
        assertThat(sql).contains("PROVIDER credential_chain", "REFRESH auto", "PROFILE 'dev''profile'")
        assertThat(sql).doesNotContain("PERSISTENT", "KEY_ID", "SECRET_ACCESS_KEY")
    }
}
