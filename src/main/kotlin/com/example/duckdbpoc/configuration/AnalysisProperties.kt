package com.example.duckdbpoc.configuration

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("analysis")
data class AnalysisProperties(val schemaLocation: String)
