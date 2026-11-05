package com.example.duckdbpoc.analysis.capacity

import org.jooq.Field
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType
import java.math.BigDecimal

/** Preserve the original Kotlin Int arithmetic and Double.toInt(), not SQL rounding. */
internal object CapacityNumbers {
    fun sumInt(value: Field<Int?>): Field<Int> {
        // The old Number.toInt() fails on null; SQL SUM must not silently skip that row.
        val number = coalesce(value.coerce(SQLDataType.INTEGER), function(
            name("error"), SQLDataType.INTEGER, inline("Null purchase order quantity cannot be converted to Int"),
        ))
        return wrapInt(sum(number).mod(BigDecimal("4294967296")).cast(SQLDataType.BIGINT))
    }

    fun subtract(left: Field<Int>, right: Field<Int>): Field<Int> =
        wrapInt(left.cast(SQLDataType.BIGINT).sub(right.cast(SQLDataType.BIGINT)))

    private fun wrapInt(value: Field<Long>): Field<Int> {
        val unsigned = value.bitAnd(4294967295L)
        return `when`(unsigned.gt(Int.MAX_VALUE.toLong()), unsigned.sub(4294967296L))
            .otherwise(unsigned).cast(SQLDataType.INTEGER)
    }

    fun toInt(value: Field<Double?>): Field<Int> =
        `when`(value.eq(Double.NaN), 0)
            .`when`(value.ge(Int.MAX_VALUE.toDouble()), Int.MAX_VALUE)
            .`when`(value.le(Int.MIN_VALUE.toDouble()), Int.MIN_VALUE)
            .otherwise(trunc(value, 0).cast(SQLDataType.INTEGER))
}
