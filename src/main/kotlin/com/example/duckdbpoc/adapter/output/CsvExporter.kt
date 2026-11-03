package com.example.duckdbpoc.adapter.output

import org.jooq.Select
import org.jooq.DSLContext
import org.jooq.conf.ParamCastMode
import org.jooq.conf.SettingsTools
import org.jooq.impl.DSL
import org.springframework.stereotype.Component

@Component
class CsvExporter {
    fun export(ctx: DSLContext, query: Select<*>, address: String) {
        val copyContext = DSL.using(ctx.configuration().derive(
            SettingsTools.clone(ctx.settings()).withParamCastMode(ParamCastMode.NEVER),
        ))
        copyContext.query("COPY ({0}) TO {1} (FORMAT CSV, HEADER true)", query, DSL.`val`(address)).execute()
    }
}
