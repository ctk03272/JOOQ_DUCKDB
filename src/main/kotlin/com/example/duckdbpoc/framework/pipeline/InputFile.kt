package com.example.duckdbpoc.framework.pipeline

data class InputFile(val address: String, val format: InputFormat)

enum class InputFormat { CSV, PARQUET }
