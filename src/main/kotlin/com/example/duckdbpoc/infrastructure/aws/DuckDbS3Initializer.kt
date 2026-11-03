package com.example.duckdbpoc.infrastructure.aws

import com.example.duckdbpoc.configuration.AwsS3Settings
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.sql.Connection

/** Runs once while creating the shared instance, before any concurrent job connections exist. */
@Component
class DuckDbS3Initializer(private val settings: AwsS3Settings) {
    fun initialize(connection: Connection) {
        if (!settings.enabled) return
        Files.createDirectories(settings.extensionDirectory)
        val ctx = DSL.using(connection, SQLDialect.DUCKDB)
        ctx.query("SET extension_directory = {0}", DSL.inline(settings.extensionDirectory.toAbsolutePath().toString())).execute()
        ctx.execute("INSTALL httpfs")
        ctx.execute("LOAD httpfs")
        ctx.execute("INSTALL aws")
        ctx.execute("LOAD aws")
        // Only metadata options are rendered; credentials are fetched by DuckDB and never logged here.
        ctx.execute(secretSql(settings))
    }

    companion object {
        internal fun secretSql(settings: AwsS3Settings): String {
            val ctx = DSL.using(SQLDialect.DUCKDB)
            fun literal(value: String) = ctx.renderInlined(DSL.inline(value))
            val options = mutableListOf("TYPE s3", "PROVIDER credential_chain", "REFRESH auto")
            settings.region?.let { options += "REGION ${literal(it)}" }
            settings.profile?.let { options += "PROFILE ${literal(it)}" }
            settings.credentialChain?.let { options += "CHAIN ${literal(it)}" }
            return "CREATE OR REPLACE SECRET app_s3 (${options.joinToString()})"
        }
    }
}
