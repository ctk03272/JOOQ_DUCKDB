package com.example.duckdbpoc.infrastructure.schema

import org.jooq.Table
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.springframework.stereotype.Component

@Component
class JobSchemaManager(private val definitions: TableDefinitions) {
    fun create(ctx: DSLContext, tables: List<Table<*>>) {
        // Parse statements structurally so schema mapping applies without string replacement.
        tables.distinct().forEach { table ->
            ctx.parser().parse(definitions.ddl(table)).queries().forEach { it.execute() }
        }
    }

    fun drop(ctx: DSLContext, jobSchema: String) {
        ctx.dropSchemaIfExists(DSL.name(jobSchema)).cascade().execute()
    }
}
