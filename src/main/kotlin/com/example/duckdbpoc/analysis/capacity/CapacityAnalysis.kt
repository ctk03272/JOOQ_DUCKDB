package com.example.duckdbpoc.analysis.capacity

import com.example.duckdbpoc.framework.analysis.DuckDbAnalysis
import com.example.duckdbpoc.generated.memory.tables.references.CAPACITY
import com.example.duckdbpoc.generated.memory.tables.references.CAPACITY_RESULT
import com.example.duckdbpoc.generated.memory.tables.references.FC_EARLY_SHUFFLING_CONFIG
import com.example.duckdbpoc.generated.memory.tables.references.FC_SHUTDOWN
import com.example.duckdbpoc.generated.memory.tables.references.PURCHASE_ORDER
import org.jooq.Field
import org.jooq.Select
import org.jooq.Table
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType

class CapacityAnalysis : DuckDbAnalysis<CapacityParameters> {
    override fun query(parameters: CapacityParameters): Select<*> {
        val po = preparePoSum()
        val remaining = applyPoToCapacity(po, parameters)
        val available = applyShutdown(remaining)
        val ratios = applyEarlyShufflingRatio(available, parameters)
        val reservation = least(
            ratios.column(CAPACITY_RESULT.REVISED_CAPACITY),
            CapacityNumbers.toInt(ratios.column(CAPACITY_RESULT.EARLY_SHUFFLING_RATIO)
                .mul(ratios.column(CAPACITY_RESULT.OLD_CAPACITY).cast(SQLDataType.DOUBLE))),
        )
        // Output order and names come from the generated result table's 20-column schema.
        return select(CAPACITY_RESULT.fields().map { field ->
            if (field == CAPACITY_RESULT.EARLY_SHUFFLING_RESERVATION) reservation.`as`(field)
            else ratios.field(field)!!
        }).from(ratios)
    }

    private fun preparePoSum(): Table<*> {
        val date = replace(PURCHASE_ORDER.DATE, inline("-"), inline("")).cast(SQLDataType.INTEGER)
        return select(
            date.`as`(CAPACITY_RESULT.DATE),
            PURCHASE_ORDER.FC.`as`(CAPACITY_RESULT.CENTER_CODE),
            PURCHASE_ORDER.TYPE.`as`(CAPACITY_RESULT.CAPACITY_TYPE),
            // Despite its old name confirmedQuantitySum, the source aggregates quantity.
            CapacityNumbers.sumInt(PURCHASE_ORDER.QUANTITY).`as`(CAPACITY_RESULT.OPEN_ORDER),
            CapacityNumbers.sumInt(PURCHASE_ORDER.MODELALLOCATEDQUANTITY).`as`(CAPACITY_RESULT.MODEL_ALLOCATED),
        ).from(PURCHASE_ORDER).groupBy(date, PURCHASE_ORDER.FC, PURCHASE_ORDER.TYPE).asTable("po_sum")
    }

    private fun applyPoToCapacity(po: Table<*>, parameters: CapacityParameters): Table<*> {
        val original = coalesce(CAPACITY.CAPACITY_, 0).coerce(SQLDataType.INTEGER)
        val revised = coalesce(CAPACITY.REVISED_CAPACITY, 0).coerce(SQLDataType.INTEGER)
        val reserved = coalesce(CAPACITY.RESERVED_CAPACITY, 0).coerce(SQLDataType.INTEGER)
        val openOrder = coalesce(po.column(CAPACITY_RESULT.OPEN_ORDER), 0).coerce(SQLDataType.INTEGER)
        val allocated = coalesce(po.column(CAPACITY_RESULT.MODEL_ALLOCATED), 0).coerce(SQLDataType.INTEGER)
        val revisedRemaining = CapacityNumbers.subtract(revised, openOrder)
        val reservedRemaining = greatest(
            `when`(revisedRemaining.gt(0), reserved)
                .`when`(CapacityNumbers.subtract(openOrder, allocated).ge(revised), CapacityNumbers.subtract(reserved, allocated))
                .otherwise(CapacityNumbers.subtract(reserved, CapacityNumbers.subtract(openOrder, revised))),
            inline(0),
        )
        return select(
            CAPACITY.DATE, CAPACITY.CENTER_CODE, CAPACITY.CENTER_MANAGEMENT_TYPE, CAPACITY.CAPACITY_TYPE,
            (if (parameters.isUnLimitCapacity) inline(Int.MAX_VALUE)
             else greatest(CapacityNumbers.subtract(original, openOrder), inline(0))).`as`(CAPACITY_RESULT.CAPACITY),
            CAPACITY.OVERFLOW_NOT_ALLOWED, CAPACITY.HARDCAP,
            (if (parameters.isUnLimitCapacity) inline(Int.MAX_VALUE)
             else greatest(revisedRemaining, inline(0))).`as`(CAPACITY_RESULT.REVISED_CAPACITY),
            (if (parameters.isUnLimitCapacity) inline(0) else reservedRemaining).`as`(CAPACITY_RESULT.RESERVED_CAPACITY),
            CAPACITY.BACKLOG_VERSION, CAPACITY.HARDCAP_TYPE,
            original.`as`(CAPACITY_RESULT.OLD_CAPACITY), original.`as`(CAPACITY_RESULT.ORIGINAL_CAPACITY),
            revised.`as`(CAPACITY_RESULT.ORIGINAL_REVISED_CAPACITY), reserved.`as`(CAPACITY_RESULT.ORIGINAL_RESERVED_CAPACITY),
            openOrder.`as`(CAPACITY_RESULT.OPEN_ORDER), allocated.`as`(CAPACITY_RESULT.MODEL_ALLOCATED),
        ).from(CAPACITY).leftJoin(po).on(
            CAPACITY.DATE.isNotDistinctFrom(po.column(CAPACITY_RESULT.DATE))
                .and(CAPACITY.CENTER_CODE.isNotDistinctFrom(po.column(CAPACITY_RESULT.CENTER_CODE)))
                .and(CAPACITY.CAPACITY_TYPE.isNotDistinctFrom(po.column(CAPACITY_RESULT.CAPACITY_TYPE))),
        ).asTable("remaining_capacity")
    }

    private fun applyShutdown(remaining: Table<*>): Table<*> {
        val shutdown = select(
            replace(FC_SHUTDOWN.SHUTDOWNDT, inline("-"), inline("")).cast(SQLDataType.INTEGER).`as`(CAPACITY_RESULT.DATE),
            FC_SHUTDOWN.CENTERCODE.`as`(CAPACITY_RESULT.CENTER_CODE),
            inline(true).`as`(CAPACITY_RESULT.BLOCKED_FC),
        ).from(FC_SHUTDOWN).asTable("shutdown")
        val blocked = coalesce(shutdown.column(CAPACITY_RESULT.BLOCKED_FC), false)
        val capacities = listOf(CAPACITY_RESULT.CAPACITY, CAPACITY_RESULT.REVISED_CAPACITY, CAPACITY_RESULT.RESERVED_CAPACITY)
        return select(remaining.asterisk().except(capacities.map { remaining.column(it) }))
            .select(capacities.map { `when`(blocked.isTrue, 0).otherwise(remaining.column(it)).`as`(it) })
            .select(blocked.`as`(CAPACITY_RESULT.BLOCKED_FC))
            .from(remaining).leftJoin(shutdown).on(
                remaining.column(CAPACITY_RESULT.DATE).isNotDistinctFrom(shutdown.column(CAPACITY_RESULT.DATE))
                    .and(remaining.column(CAPACITY_RESULT.CENTER_CODE).isNotDistinctFrom(shutdown.column(CAPACITY_RESULT.CENTER_CODE))),
            ).asTable("available_capacity")
    }

    private fun applyEarlyShufflingRatio(available: Table<*>, parameters: CapacityParameters): Table<*> {
        val config = FC_EARLY_SHUFFLING_CONFIG
        // Kotlin isNullOrBlank includes tabs and Unicode space/separator characters.
        val blank = config.CAPACITYTYPE.isNull.or(
            config.CAPACITYTYPE.likeRegex("^[\\s\\p{Z}\\x0B\\x1C-\\x1F]*$"),
        )
        // distinctBy keeps the first input row, including a null ratio on that row.
        val specific = select(config.FC, config.CAPACITYTYPE, config.EARLYSHUFFLINGRATIO)
            .from(config).where(blank.not())
            .qualify(rowNumber().over().partitionBy(config.FC, config.CAPACITYTYPE).orderBy(config.rowid()).eq(1))
            .asTable("specific_config")
        val wildcard = select(config.FC, config.EARLYSHUFFLINGRATIO)
            .from(config).where(blank)
            .qualify(rowNumber().over().partitionBy(config.FC).orderBy(config.rowid()).eq(1))
            .asTable("wildcard_config")
        val excludedRc = if (parameters.useEarlyShufflingForPicoRC)
            parameters.rcList - parameters.picoRcList.toSet() else parameters.rcList
        val excluded = available.column(CAPACITY_RESULT.CENTER_CODE).`in`(excludedRc + parameters.xDockList)
        val fallback = `when`(excluded, 0.0).otherwise(parameters.defaultEarlyShufflingRatio)
        val ratio = if (parameters.isUnLimitCapacity) inline(0.0) else coalesce(
            specific.column(config.EARLYSHUFFLINGRATIO), wildcard.column(config.EARLYSHUFFLINGRATIO), fallback,
        )
        return select(available.asterisk()).select(ratio.`as`(CAPACITY_RESULT.EARLY_SHUFFLING_RATIO))
            .from(available)
            .leftJoin(specific).on(available.column(CAPACITY_RESULT.CENTER_CODE).isNotDistinctFrom(specific.column(config.FC))
                .and(available.column(CAPACITY_RESULT.CAPACITY_TYPE).isNotDistinctFrom(specific.column(config.CAPACITYTYPE))))
            .leftJoin(wildcard).on(available.column(CAPACITY_RESULT.CENTER_CODE).isNotDistinctFrom(wildcard.column(config.FC)))
            .asTable("capacity_with_ratio")
    }

    private fun <T> Table<*>.column(field: Field<T>): Field<T> = field(field)!!
}
