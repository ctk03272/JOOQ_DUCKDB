import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.9.25"
    kotlin("plugin.spring") version "1.9.25"
    id("org.springframework.boot") version "3.5.16"
    id("org.jooq.jooq-codegen-gradle") version "3.21.9"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

repositories { mavenCentral() }

val jooqVersion = "3.21.9"
val duckdbVersion = "2.0.0.1"
val schemaFiles = fileTree("src/main/resources/db") { include("**/*.sql") }
val generatedSources = layout.projectDirectory.dir("src/main/kotlin")

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:3.5.16"))
    implementation("org.springframework.boot:spring-boot-starter")
    implementation(kotlin("reflect"))
    implementation("org.jooq:jooq:$jooqVersion")
    implementation("org.duckdb:duckdb_jdbc:$duckdbVersion")
    jooqCodegen("org.duckdb:duckdb_jdbc:$duckdbVersion")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    jvmToolchain(21)
}

jooq {
    configuration {
        jdbc {
            driver = "org.duckdb.DuckDBDriver"
            url = "jdbc:duckdb:"
            initScript = schemaFiles.files.sortedBy { it.path }.joinToString("\n") { it.readText() }
        }
        generator {
            name = "org.jooq.codegen.KotlinGenerator"
            database {
                name = "org.jooq.meta.duckdb.DuckDBDatabase"
                inputSchema = "pipeline"
                includes = ".*"
            }
            target {
                packageName = "com.example.duckdbpoc.generated"
                directory = generatedSources.asFile.absolutePath
                isClean = true
            }
        }
    }
}

tasks.named("jooqCodegen") { inputs.files(schemaFiles) }
tasks.withType<KotlinCompile>().configureEach {
    kotlinOptions {
        jvmTarget = "21"
        freeCompilerArgs += "-Xjsr305=strict"
    }
}
tasks.named("compileKotlin") { dependsOn("jooqCodegen") }
tasks.withType<Test>().configureEach { useJUnitPlatform() }

// Load generation is opt-in and uses only the test source set.
tasks.named<Test>("test") {
    useJUnitPlatform { excludeTags("load", "s3") }
}
tasks.register<Test>("loadTest") {
    description = "Run the independent concurrent pipeline load test"
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform { includeTags("load") }
    systemProperty("load.jobs", providers.gradleProperty("loadJobs").getOrElse("100"))
    outputs.upToDateWhen { false }
    shouldRunAfter(tasks.named("test"))
}

// Real S3 data IO is opt-in; Spring tests still initialize the actual AWS configuration.
tasks.register<Test>("s3Test") {
    description = "Opt-in test of real DuckDB S3 input and output"
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform { includeTags("s3") }
    outputs.upToDateWhen { false }
    shouldRunAfter(tasks.named("test"))
}
