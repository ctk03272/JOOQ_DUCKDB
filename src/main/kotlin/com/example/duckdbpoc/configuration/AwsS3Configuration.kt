package com.example.duckdbpoc.configuration

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.nio.file.Path

/** Configure these in Kotlin for deployment. No access keys are stored in this project. */
data class AwsS3Settings(
    val enabled: Boolean = false,
    val region: String? = null,
    val profile: String? = null,
    val credentialChain: String? = null,
    val extensionDirectory: Path = Path.of("build/duckdb-extensions"),
)

@Configuration
class AwsS3Configuration {
    @Bean
    fun awsS3Settings() = AwsS3Settings(
        enabled = true,
        region = "ap-northeast-2",
        profile = "duckdb-poc",
        credentialChain = null,
    )
}
