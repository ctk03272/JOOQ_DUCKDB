package com.example.duckdbpoc.configuration

import com.example.duckdbpoc.infrastructure.aws.DuckDbS3Initializer
import org.duckdb.DuckDBConnection
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.nio.file.Files
import java.sql.DriverManager
import java.util.Properties

@Configuration
class DuckDbConfiguration {
    @Bean(destroyMethod = "close")
    fun duckDbConnection(properties: DuckDbProperties, s3: DuckDbS3Initializer): DuckDBConnection {
        Files.createDirectories(properties.spillDirectory)
        val options = Properties().apply {
            setProperty("memory_limit", properties.memoryLimit)
            setProperty("threads", properties.threads.toString())
            setProperty("temp_directory", properties.spillDirectory.toAbsolutePath().toString())
        }
        val connection = DriverManager.getConnection("jdbc:duckdb:", options) as DuckDBConnection
        try {
            s3.initialize(connection)
            return connection
        } catch (error: Throwable) {
            try { connection.close() } catch (cleanup: Throwable) { error.addSuppressed(cleanup) }
            throw error
        }
    }
}
