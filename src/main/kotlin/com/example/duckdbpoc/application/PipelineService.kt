package com.example.duckdbpoc.application

import com.example.duckdbpoc.framework.analysis.AnalysisRegistry
import com.example.duckdbpoc.framework.pipeline.AnalysisRequest
import com.example.duckdbpoc.framework.pipeline.PipelineExecutor
import org.springframework.stereotype.Service

@Service
class PipelineService(
    private val executor: PipelineExecutor,
    private val registry: AnalysisRegistry,
) {
    fun <P> process(request: AnalysisRequest<P>) = registry.get(request.analysisId).let { analysis ->
        val snapshot = request.copy(inputs = request.inputs.toMap())
        executor.execute(analysis, snapshot, snapshot.output)
    }
}
