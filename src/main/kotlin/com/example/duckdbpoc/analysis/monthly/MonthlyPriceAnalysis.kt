package com.example.duckdbpoc.analysis.monthly

import com.example.duckdbpoc.framework.analysis.DuckDbAnalysis
import com.example.duckdbpoc.generated.memory.tables.references.MONTHLY_PRICE
import com.example.duckdbpoc.generated.memory.tables.references.PRICES
import com.example.duckdbpoc.generated.memory.tables.references.READINGS
import org.jooq.impl.DSL.*

class MonthlyPriceAnalysis : DuckDbAnalysis<Unit> {
    override fun query(parameters: Unit) = select(
        READINGS.SYSTEM_ID.`as`(MONTHLY_PRICE.SYSTEM_ID),
        year(READINGS.READ_ON).`as`(MONTHLY_PRICE.READING_YEAR),
        month(READINGS.READ_ON).`as`(MONTHLY_PRICE.READING_MONTH),
        sum(READINGS.POWER.mul(PRICES.VALUE)).`as`(MONTHLY_PRICE.PRICED_POWER_SUM),
    ).from(READINGS)
        .join(PRICES).on(PricePeriod.matchesReading())
        .groupBy(READINGS.SYSTEM_ID, year(READINGS.READ_ON), month(READINGS.READ_ON))
}
