package com.example.duckdbpoc.framework.analysis

import org.springframework.stereotype.Component

@Component
class AnalysisRegistry(configurations: List<ConfiguredAnalysis<*>>) {
    private val definitions = configurations.associateBy { it.id }
    // The same identity-based AnalysisId<P> can only register ConfiguredAnalysis<P>.
    // The heterogeneous map erases P; restore it only at this lookup boundary.
    @Suppress("UNCHECKED_CAST")
    fun <P> get(id: AnalysisId<P>): ConfiguredAnalysis<P> = definitions.getValue(id) as ConfiguredAnalysis<P>
}
