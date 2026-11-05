package com.example.duckdbpoc.adapter.http

import com.example.duckdbpoc.analysis.capacity.CapacityAnalysisId
import com.example.duckdbpoc.analysis.capacity.CapacityParameters
import com.example.duckdbpoc.application.PipelineService
import com.example.duckdbpoc.framework.pipeline.AnalysisRequest
import com.example.duckdbpoc.framework.pipeline.InputFile
import com.example.duckdbpoc.framework.pipeline.InputFormat
import com.example.duckdbpoc.framework.pipeline.PipelineResult
import com.example.duckdbpoc.generated.memory.tables.references.CAPACITY
import com.example.duckdbpoc.generated.memory.tables.references.FC_SHUTDOWN
import com.example.duckdbpoc.generated.memory.tables.references.PURCHASE_ORDER
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/analyses/capacity")
class CapacityController(private val pipelineService: PipelineService) {
    @PostMapping
    fun process(@RequestBody request: CapacityHttpRequest): PipelineResult = pipelineService.process(
        AnalysisRequest(
            analysisId = CapacityAnalysisId.CAPACITY,
            inputs = mapOf(
                CAPACITY to InputFile(request.capacityUri, InputFormat.CSV),
                PURCHASE_ORDER to InputFile(request.purchaseOrderUri, InputFormat.CSV),
                FC_SHUTDOWN to InputFile(request.shutdownUri, InputFormat.CSV),
            ),
            output = request.outputUri,
            parameters = CapacityParameters(
                rcList = request.rcList,
                xDockList = request.xDockList,
                picoRcList = request.picoRcList,
                defaultEarlyShufflingRatio = request.defaultEarlyShufflingRatio,
                isUnLimitCapacity = request.isUnLimitCapacity,
                useEarlyShufflingForPicoRC = request.useEarlyShufflingForPicoRC,
            ),
        ),
    )
}
