package com.example.duckdbpoc.adapter.input

import com.example.duckdbpoc.framework.pipeline.InputFile
import com.example.duckdbpoc.framework.pipeline.InputFormat
import org.jooq.DSLContext
import org.jooq.Table
import org.springframework.stereotype.Component

@Component
class InputLoader(private val csv: CsvLoader, private val parquet: ParquetLoader) {
    fun load(ctx: DSLContext, target: Table<*>, source: InputFile) = when (source.format) {
        InputFormat.CSV -> csv.load(ctx, target, source.address)
        InputFormat.PARQUET -> parquet.load(ctx, target, source.address)
    }
}
