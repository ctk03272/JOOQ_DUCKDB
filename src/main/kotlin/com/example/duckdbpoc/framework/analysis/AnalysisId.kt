package com.example.duckdbpoc.framework.analysis

/** Declare once and share between registration and requests. Identity carries the parameter type. */
class AnalysisId<P>(val value: String) {
    override fun toString(): String = value
}
