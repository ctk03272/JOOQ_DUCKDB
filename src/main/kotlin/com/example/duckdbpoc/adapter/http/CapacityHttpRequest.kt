package com.example.duckdbpoc.adapter.http

/** CSV input addresses and calculation parameters supplied by the HTTP caller. */
data class CapacityHttpRequest(
    val capacityUri: String,
    val purchaseOrderUri: String,
    val shutdownUri: String,
    val outputUri: String,
    val rcList: List<String>,
    val xDockList: List<String>,
    val picoRcList: List<String>,
    val defaultEarlyShufflingRatio: Double,
    val isUnLimitCapacity: Boolean,
    val useEarlyShufflingForPicoRC: Boolean,
)
