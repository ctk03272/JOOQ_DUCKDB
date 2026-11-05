package com.example.duckdbpoc

import com.example.duckdbpoc.analysis.monthly.MonthlyAnalyses
import com.example.duckdbpoc.framework.analysis.AnalysisId

import com.example.duckdbpoc.application.PipelineService
import com.example.duckdbpoc.framework.analysis.AnalysisRegistry
import com.example.duckdbpoc.framework.pipeline.InputFile
import com.example.duckdbpoc.framework.pipeline.InputFormat
import com.example.duckdbpoc.framework.pipeline.AnalysisRequest
import com.example.duckdbpoc.framework.pipeline.PipelineExecutor
import java.nio.file.Path

internal val String.localPath: Path get() = Path.of(this)

/** Test fixture wiring only. Actual requests carry individual file locations. */
internal fun executeFixture(
    registry: AnalysisRegistry,
    executor: PipelineExecutor,
    id: AnalysisId<Unit>,
    directory: Path,
    output: Path,
) = registry.get(id).let { original ->
    java.nio.file.Files.createDirectories(output.parent)
    PipelineService(executor, registry).process(
        AnalysisRequest(id, original.requiredInputs.associateWith {
            InputFile(directory.resolve("${it.name}.parquet").toString(), InputFormat.PARQUET)
        }, output = output.toString(), parameters = Unit),
    )
}

// These fixtures have one record per line. Preserve header order and duplicate data rows.
internal fun csvContents(path: Path): Pair<String, List<String>> =
    java.nio.file.Files.readAllLines(path).let { lines -> lines.first() to lines.drop(1).sorted() }
