package com.example.duckdbpoc.framework.pipeline

import org.jooq.Table
import java.util.UUID

data class AnalysisRequest(
    val analysisId: String,
    val inputs: Map<Table<*>, InputFile>,
    val output: String,
    val requestId: String = UUID.randomUUID().toString(),
)
