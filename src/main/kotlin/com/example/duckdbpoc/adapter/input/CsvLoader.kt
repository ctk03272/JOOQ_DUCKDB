package com.example.duckdbpoc.adapter.input

import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.impl.DSL
import org.springframework.stereotype.Component

@Component
class CsvLoader {
    fun load(ctx: DSLContext, target: Table<*>, source: String) {
        val columns = DSL.list(*target.fields().map { DSL.name(it.name) }.toTypedArray())
        // Read header names, then let the declared target columns perform type conversion.
        // all_varchar avoids sampled type inference changing the input contract.
        ctx.query(
            "INSERT INTO {0} ({1}) SELECT {1} FROM read_csv({2}, header = true, delim = ',', all_varchar = true)",
            target, columns, DSL.`val`(source),
        ).execute()
    }
}
