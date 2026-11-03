package com.example.duckdbpoc.analysis.monthly

import com.example.duckdbpoc.generated.memory.tables.references.PRICES
import com.example.duckdbpoc.generated.memory.tables.references.READINGS
import org.jooq.impl.SQLDataType

internal object PricePeriod {
    fun matchesReading() = READINGS.READ_ON.cast(SQLDataType.LOCALDATE).let { date ->
        date.ge(PRICES.VALID_FROM).and(date.lt(PRICES.VALID_UNTIL))
    }
}
