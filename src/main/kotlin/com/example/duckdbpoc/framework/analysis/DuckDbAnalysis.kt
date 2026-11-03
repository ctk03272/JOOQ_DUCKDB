package com.example.duckdbpoc.framework.analysis

import org.jooq.Select

/** A stateless query definition. Build a fresh query on every invocation. */
interface DuckDbAnalysis {
    fun query(): Select<*>
}
