package com.example.duckdbpoc.framework.analysis

import org.jooq.Schema
import org.jooq.Table

/** Analysis definition only; callers supply input and output addresses. */
data class ConfiguredAnalysis(
    val id: String,
    val schema: Schema,
    val query: DuckDbAnalysis,
    val requiredInputs: List<Table<*>>,
    val outputTable: Table<*>,
)
