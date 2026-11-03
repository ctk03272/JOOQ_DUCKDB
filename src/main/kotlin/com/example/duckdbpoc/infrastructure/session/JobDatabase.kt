package com.example.duckdbpoc.infrastructure.session

import com.example.duckdbpoc.infrastructure.jooq.JobContextFactory
import com.example.duckdbpoc.infrastructure.schema.JobSchemaManager
import org.duckdb.DuckDBConnection
import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.Schema
import org.springframework.stereotype.Component
import java.util.UUID

/** Owns only the job connection/schema lifetime. Never stores a current job in singleton state. */
@Component
class JobDatabase(
    private val anchor: DuckDBConnection,
    private val contexts: JobContextFactory,
    private val schemas: JobSchemaManager,
) {
    fun <T> use(definition: Schema, tables: List<Table<*>>, block: (DSLContext) -> T): T = anchor.duplicate().use { connection ->
        val schema = "job_" + UUID.randomUUID().toString().replace("-", "")
        val ctx = contexts.create(connection, definition.name, schema)
        var failure: Throwable? = null
        try {
            ctx.createSchema(definition).execute()
            schemas.create(ctx, tables)
            block(ctx)
        } catch (error: Throwable) {
            failure = error
            throw error
        } finally {
            try { schemas.drop(ctx, schema) } catch (cleanup: Throwable) {
                if (failure != null) failure.addSuppressed(cleanup) else throw cleanup
            }
        }
    }
}
