package com.example.duckdbpoc.framework.analysis

import org.springframework.stereotype.Component

@Component
class AnalysisRegistry(configurations: List<ConfiguredAnalysis>) {
    private val definitions = configurations.associateBy { it.id }
    fun get(id: String): ConfiguredAnalysis = definitions.getValue(id)
}
