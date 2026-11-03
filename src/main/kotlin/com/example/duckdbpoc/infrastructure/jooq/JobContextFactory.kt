package com.example.duckdbpoc.infrastructure.jooq

import org.jooq.SQLDialect
import org.jooq.conf.MappedSchema
import org.jooq.conf.RenderMapping
import org.jooq.conf.Settings
import org.jooq.impl.DSL
import org.springframework.stereotype.Component
import java.sql.Connection

@Component
class JobContextFactory {
    fun create(connection: Connection, logicalSchema: String, jobSchema: String) = DSL.using(
        connection, SQLDialect.DUCKDB,
        Settings().withRenderCatalog(false).withExecuteLogging(false).withRenderMapping(
            RenderMapping().withSchemata(MappedSchema().withInput(logicalSchema).withOutput(jobSchema)),
        ),
    )
}
