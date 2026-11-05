package com.example.duckdbpoc

import com.example.duckdbpoc.analysis.capacity.CapacityParameters
import com.example.duckdbpoc.framework.pipeline.InputFile
import com.example.duckdbpoc.framework.pipeline.InputFormat
import com.example.duckdbpoc.generated.memory.tables.references.*
import org.jooq.Table
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection

internal data class CapacityRow(
    val center: String?, val capacity: Int? = 100, val revised: Int? = 80, val reserved: Int? = 20,
    val date: Int = 20261105, val type: String? = "A",
)
internal data class OrderRow(
    val center: String?, val quantity: Int, val allocated: Int, val date: String = "2026-11-05", val type: String? = "A",
)
internal data class ShutdownRow(val center: String?, val date: String = "2026-11-05")
internal data class RatioRow(val center: String?, val type: String?, val ratio: Double?)

internal data class CapacityFixture(
    val capacities: List<CapacityRow>,
    val orders: List<OrderRow> = emptyList(),
    val shutdowns: List<ShutdownRow> = emptyList(),
    val ratios: List<RatioRow>? = null,
) {
    fun write(directory: Path): Map<Table<*>, InputFile> {
        Files.createDirectories(directory)
        fun file(table: Table<*>, rows: List<List<Any?>>): InputFile {
            val path = directory.resolve("${table.name}.csv")
            val lines = listOf(table.fields().map { it.name }) + rows
            Files.writeString(path, lines.joinToString("\n", postfix = "\n") { row ->
                row.joinToString(",") { value -> when (value) {
                    null -> ""
                    is String -> "\"${value.replace("\"", "\"\"")}\""
                    else -> value.toString()
                } }
            })
            return InputFile(path.toString(), InputFormat.CSV)
        }
        val inputs = linkedMapOf<Table<*>, InputFile>(
            CAPACITY to file(CAPACITY, capacities.map {
                listOf(it.date, it.center, "managed", it.type, it.capacity, true, 1.5, it.revised, it.reserved, 2.5, "hard")
            }),
            PURCHASE_ORDER to file(PURCHASE_ORDER, orders.mapIndexed { index, it ->
                // confirmedQuantity is deliberately different from quantity.
                listOf(it.date, index.toLong(), it.center, it.type, it.quantity, 999999, it.allocated)
            }),
            FC_SHUTDOWN to file(FC_SHUTDOWN, shutdowns.map { listOf(it.center, it.date) }),
        )
        ratios?.let { rows ->
            inputs[FC_EARLY_SHUFFLING_CONFIG] = file(FC_EARLY_SHUFFLING_CONFIG,
                rows.map { listOf(it.center, it.type, it.ratio, "already-filtered") })
        }
        return inputs
    }

    /** Test-only scalar transcription of the supplied photos; no new DataFrame dependency. */
    fun expected(p: CapacityParameters): List<List<Any?>> {
        val sums = orders.groupBy { Triple(it.date.replace("-", "").toInt(), it.center, it.type) }
        val specific = ratios.orEmpty().filter { !it.type.isNullOrBlank() }.distinctBy { it.center to it.type }
        val wildcard = ratios.orEmpty().filter { it.type.isNullOrBlank() }.distinctBy { it.center }
        val excludedRc = if (p.useEarlyShufflingForPicoRC) p.rcList - p.picoRcList.toSet() else p.rcList
        return capacities.flatMap { c ->
            val po = sums[Triple(c.date, c.center, c.type)].orEmpty()
            val open = po.sumOf { it.quantity }
            val model = po.sumOf { it.allocated }
            val original = c.capacity ?: 0
            val revised = c.revised ?: 0
            val reserved = c.reserved ?: 0
            var remaining = (original - open).coerceAtLeast(0)
            var revisedRemaining = (revised - open).coerceAtLeast(0)
            var reservedRemaining = (if (revised - open > 0) reserved
                else if (open - model >= revised) reserved - model
                else reserved - (open - revised)).coerceAtLeast(0)
            if (p.isUnLimitCapacity) {
                remaining = Int.MAX_VALUE
                revisedRemaining = Int.MAX_VALUE
                reservedRemaining = 0
            }
            val matches = shutdowns.filter { it.date.replace("-", "").toInt() == c.date && it.center == c.center }
            val blocked = matches.isNotEmpty()
            if (blocked) { remaining = 0; revisedRemaining = 0; reservedRemaining = 0 }
            val ratio = if (p.isUnLimitCapacity) 0.0 else
                specific.find { it.center == c.center && it.type == c.type }?.ratio
                    ?: wildcard.find { it.center == c.center }?.ratio
                    ?: if (c.center in excludedRc || c.center in p.xDockList) 0.0 else p.defaultEarlyShufflingRatio
            val reservation = revisedRemaining.coerceAtMost((ratio * original).toInt())
            val row = listOf(c.date, c.center, "managed", c.type, remaining, true, 1.5,
                revisedRemaining, reservedRemaining, 2.5, "hard", original, original, revised, reserved,
                open, model, reservation, ratio, blocked)
            List(matches.size.coerceAtLeast(1)) { row }
        }
    }
}

internal fun readCapacityCsv(connection: Connection, path: Path): List<List<Any?>> =
    connection.prepareStatement("SELECT * FROM read_csv(?, header=true, all_varchar=true)").use { statement ->
        statement.setString(1, path.toString())
        statement.executeQuery().use { rows ->
            buildList {
                while (rows.next()) add(CAPACITY_RESULT.fields().mapIndexed { index, field ->
                    val value = rows.getString(index + 1)
                    when {
                        value == null -> null
                        field.dataType.isInteger -> value.toInt()
                        field.dataType.isBoolean -> value.toBooleanStrict()
                        field.dataType.isNumeric -> when (value.lowercase()) {
                            "nan" -> Double.NaN
                            "inf", "infinity" -> Double.POSITIVE_INFINITY
                            "-inf", "-infinity" -> Double.NEGATIVE_INFINITY
                            else -> value.toDouble()
                        }
                        else -> value
                    }
                })
            }
        }
    }
