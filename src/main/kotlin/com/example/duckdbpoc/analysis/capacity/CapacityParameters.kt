package com.example.duckdbpoc.analysis.capacity

data class CapacityParameters(
    val rcList: List<String>,
    val xDockList: List<String>,
    val picoRcList: List<String>,
    val defaultEarlyShufflingRatio: Double,
    val isUnLimitCapacity: Boolean,
    val useEarlyShufflingForPicoRC: Boolean,
)
