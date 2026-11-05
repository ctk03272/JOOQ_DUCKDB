package com.example.duckdbpoc.analysis.monthly

import com.example.duckdbpoc.framework.analysis.DuckDbAnalysis
import com.example.duckdbpoc.generated.memory.tables.references.MONTHLY_USAGE
import com.example.duckdbpoc.generated.memory.tables.references.READINGS
import com.example.duckdbpoc.generated.memory.tables.references.SYSTEMS
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType

class MonthlyUsageAnalysis : DuckDbAnalysis<Unit> {
    override fun query(parameters: Unit) = select(
        SYSTEMS.ID.`as`(MONTHLY_USAGE.SYSTEM_ID),
        SYSTEMS.NAME.`as`(MONTHLY_USAGE.SYSTEM_NAME),
        year(READINGS.READ_ON).`as`(MONTHLY_USAGE.READING_YEAR),
        month(READINGS.READ_ON).`as`(MONTHLY_USAGE.READING_MONTH),
        count().cast(SQLDataType.BIGINT).`as`(MONTHLY_USAGE.READING_COUNT),
        sum(READINGS.POWER).`as`(MONTHLY_USAGE.POWER_SUM),
        avg(READINGS.POWER).`as`(MONTHLY_USAGE.POWER_AVERAGE),
    ).from(READINGS)
        .join(SYSTEMS).on(READINGS.SYSTEM_ID.eq(SYSTEMS.ID))
        .groupBy(SYSTEMS.ID, SYSTEMS.NAME, year(READINGS.READ_ON), month(READINGS.READ_ON))
}
