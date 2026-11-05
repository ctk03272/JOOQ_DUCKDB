package com.example.duckdbpoc.framework.analysis

import org.jooq.Schema
import org.jooq.Table

/** Analysis definition only; callers supply input and output addresses. */
data class ConfiguredAnalysis<P>(
    val id: AnalysisId<P>,
    val schema: Schema,
    val query: DuckDbAnalysis<P>,
    val requiredInputs: List<Table<*>>,
    val outputTable: Table<*>,
    val optionalInputs: List<Table<*>> = emptyList(),
)
