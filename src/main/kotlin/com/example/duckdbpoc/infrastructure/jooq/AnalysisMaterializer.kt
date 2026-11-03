package com.example.duckdbpoc.infrastructure.jooq

import org.jooq.DSLContext
import org.jooq.Select
import org.jooq.Table
import org.springframework.stereotype.Component

@Component
class AnalysisMaterializer {
    fun store(ctx: DSLContext, target: Table<*>, query: Select<*>) {
        ctx.insertInto(target).columns(*target.fields()).select(query).execute()
    }
}
