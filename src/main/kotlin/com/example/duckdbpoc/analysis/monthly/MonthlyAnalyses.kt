package com.example.duckdbpoc.analysis.monthly

import com.example.duckdbpoc.framework.analysis.AnalysisId

object MonthlyAnalyses {
    val USAGE = AnalysisId<Unit>("monthly-usage")
    val PRICE = AnalysisId<Unit>("monthly-price")
}
