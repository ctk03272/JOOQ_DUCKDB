package com.example.duckdbpoc.framework.pipeline

import org.jooq.Table
import com.example.duckdbpoc.framework.analysis.AnalysisId
import java.util.UUID

data class AnalysisRequest<P>(
    val analysisId: AnalysisId<P>,
    val inputs: Map<Table<*>, InputFile>,
    val output: String,
    val parameters: P,
    val requestId: String = UUID.randomUUID().toString(),
)
