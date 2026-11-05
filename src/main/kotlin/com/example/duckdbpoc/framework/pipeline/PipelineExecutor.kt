package com.example.duckdbpoc.framework.pipeline

import com.example.duckdbpoc.framework.analysis.ConfiguredAnalysis
import com.example.duckdbpoc.adapter.input.InputLoader
import com.example.duckdbpoc.adapter.output.CsvExporter
import com.example.duckdbpoc.infrastructure.jooq.AnalysisMaterializer
import com.example.duckdbpoc.infrastructure.session.JobDatabase
import org.jooq.impl.DSL
import org.springframework.stereotype.Component

/** Executes definitions; has no dependency on monthly analysis or its generated tables. */
@Component
class PipelineExecutor(
    private val jobs: JobDatabase,
    private val loader: InputLoader,
    private val materializer: AnalysisMaterializer,
    private val exporter: CsvExporter,
) {
    fun <P> execute(definition: ConfiguredAnalysis<P>, request: AnalysisRequest<P>, output: String): PipelineResult = jobs.use(definition.schema, definition.requiredInputs + definition.outputTable) { ctx ->
        ctx.transaction { configuration ->
            val tx = DSL.using(configuration)
            definition.requiredInputs.forEach { loader.load(tx, it, request.inputs.getValue(it)) }
            materializer.store(tx, definition.outputTable, definition.query.query(request.parameters))
        }
        exporter.export(ctx, ctx.selectFrom(definition.outputTable), output)
        PipelineResult(output)
    }
}
