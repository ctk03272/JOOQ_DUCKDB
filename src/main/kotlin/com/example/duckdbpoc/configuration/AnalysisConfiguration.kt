package com.example.duckdbpoc.configuration

import com.example.duckdbpoc.analysis.monthly.MonthlyPriceAnalysis
import com.example.duckdbpoc.analysis.monthly.MonthlyUsageAnalysis
import com.example.duckdbpoc.framework.analysis.ConfiguredAnalysis
import com.example.duckdbpoc.generated.memory.Pipeline
import com.example.duckdbpoc.generated.memory.tables.references.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AnalysisConfiguration {
    @Bean
    fun monthlyUsage() = ConfiguredAnalysis(
        id = "monthly-usage",
        schema = Pipeline.PIPELINE,
        query = MonthlyUsageAnalysis(),
        requiredInputs = listOf(SYSTEMS, READINGS),
        outputTable = MONTHLY_USAGE,
    )

    @Bean
    fun monthlyPrice() = ConfiguredAnalysis(
        id = "monthly-price",
        schema = Pipeline.PIPELINE,
        query = MonthlyPriceAnalysis(),
        requiredInputs = listOf(PRICES, READINGS),
        outputTable = MONTHLY_PRICE,
    )
}
