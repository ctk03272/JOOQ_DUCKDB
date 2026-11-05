package com.example.duckdbpoc.configuration

import com.example.duckdbpoc.analysis.monthly.MonthlyPriceAnalysis
import com.example.duckdbpoc.analysis.monthly.MonthlyUsageAnalysis
import com.example.duckdbpoc.analysis.monthly.MonthlyAnalyses
import com.example.duckdbpoc.analysis.capacity.CapacityAnalysisId
import com.example.duckdbpoc.analysis.capacity.CapacityAnalysis
import com.example.duckdbpoc.framework.analysis.ConfiguredAnalysis
import com.example.duckdbpoc.generated.memory.Pipeline
import com.example.duckdbpoc.generated.memory.tables.references.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AnalysisConfiguration {
    @Bean
    fun capacity() = ConfiguredAnalysis(
        id = CapacityAnalysisId.CAPACITY,
        schema = Pipeline.PIPELINE,
        query = CapacityAnalysis(),
        requiredInputs = listOf(CAPACITY, PURCHASE_ORDER, FC_SHUTDOWN),
        optionalInputs = listOf(FC_EARLY_SHUFFLING_CONFIG),
        outputTable = CAPACITY_RESULT,
    )

    @Bean
    fun monthlyUsage() = ConfiguredAnalysis(
        id = MonthlyAnalyses.USAGE,
        schema = Pipeline.PIPELINE,
        query = MonthlyUsageAnalysis(),
        requiredInputs = listOf(SYSTEMS, READINGS),
        outputTable = MONTHLY_USAGE,
    )

    @Bean
    fun monthlyPrice() = ConfiguredAnalysis(
        id = MonthlyAnalyses.PRICE,
        schema = Pipeline.PIPELINE,
        query = MonthlyPriceAnalysis(),
        requiredInputs = listOf(PRICES, READINGS),
        outputTable = MONTHLY_PRICE,
    )
}
