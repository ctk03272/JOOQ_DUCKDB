package com.example.duckdbpoc.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import java.nio.file.Path

@ConfigurationProperties("duckdb")
data class DuckDbProperties(
    val memoryLimit: String,
    val threads: Int,
    val spillDirectory: Path,
)
