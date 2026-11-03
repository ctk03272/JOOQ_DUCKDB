package com.example.duckdbpoc.adapter.input

import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.impl.DSL
import org.springframework.stereotype.Component

@Component
class ParquetLoader {
    fun load(ctx: DSLContext, target: Table<*>, source: String) {
        val columns = DSL.list(*target.fields().map { DSL.name(it.name) }.toTypedArray())
        // Explicit column names protect against a different physical column order in Parquet.
        ctx.query(
            "INSERT INTO {0} ({1}) SELECT {1} FROM read_parquet({2})",
            target, columns, DSL.`val`(source),
        ).execute()
    }
}
