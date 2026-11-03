package com.example.duckdbpoc.infrastructure.schema

import com.example.duckdbpoc.configuration.AnalysisProperties
import org.jooq.SQLDialect
import org.jooq.Table
import org.jooq.impl.DSL
import org.jooq.impl.QOM
import org.springframework.core.io.support.PathMatchingResourcePatternResolver
import org.springframework.stereotype.Component

/** Domain files are read once. Jobs use selected CREATE TABLE statements, not file boundaries. */
@Component
class TableDefinitions(properties: AnalysisProperties) {
    private val definitions = DSL.using(SQLDialect.DUCKDB).let { parserContext ->
        PathMatchingResourcePatternResolver().getResources("classpath*:${properties.schemaLocation}")
            .sortedBy { it.url.toString() }.flatMap { resource ->
            val sql = resource.inputStream.bufferedReader().use { it.readText() }
            parserContext.parser().parse(sql).queries().filterIsInstance<QOM.CreateTable>()
        }.associate { query ->
            val table = query.`$table`()
            (table.schema!!.name to table.name) to parserContext.renderInlined(query)
        }
    }

    fun ddl(table: Table<*>): String = definitions.getValue(table.schema!!.name to table.name)
}
