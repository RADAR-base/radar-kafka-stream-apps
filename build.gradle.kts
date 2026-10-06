import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    application
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.radar.dependency.management)
    alias(libs.plugins.radar.publishing)
    alias(libs.plugins.radar.kotlin)
    alias(libs.plugins.radar.root.project)
    alias(libs.plugins.version.catalog.update)
    alias(libs.plugins.build.config)
    `jvm-test-suite`
    kotlin("kapt") version "1.9.24"
}

description = "RADAR-base Kafka Streams applications, including a rule engine for just-in-time interventions."

radarRootProject {
    projectVersion.set(properties["projectVersion"] as String)
    gradleVersion.set(properties["gradleVersion"] as String)
}

radarPublishing {
    val githubRepoName = "RADAR-base/radar-kafka-stream-apps"
    githubUrl.set("https://github.com/$githubRepoName.git")
    developers {
        developer {
            id.set("pvannierop")
            name.set("Pim van Nierop")
            email.set("pim@thehyve.nl")
            organization.set("The Hyve")
        }
    }
}

buildConfig {
    buildConfigField("String", "version", "\"${properties["projectVersion"] as String}\"")
}

radarKotlin {
    log4j2Version.set(libs.versions.log4j)
    sentryEnabled.set(true)
    openTelemetryAgentEnabled.set(false)
}

ktlint {
    filter {
        exclude { it.file.path.contains("${File.separator}build${File.separator}generated${File.separator}") }
    }
}

application {
    mainClass.set("org.radarbase.RadarBackend")
}

dependencies {
    implementation(libs.radar.commons)
    implementation(libs.radar.commons.server)
    implementation(libs.avro)
    implementation(libs.cel.core)

    implementation(libs.kotlin.stdlib)

    implementation(libs.kafka.streams)
    implementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "jline", module = "jline")
        exclude(group = "io.netty", module = "netty")
    }

    implementation(libs.commons.cli)
    implementation(libs.jackson.annotations)
    implementation(libs.jackson.module.kotlin)

    // Testing
    testImplementation(libs.mockito.core)

    testImplementation(kotlin("test"))
}

// --- Vulnerability fixes start ---
configurations.all {
    resolutionStrategy.dependencySubstitution {
        // Substitute the old group/module with drop-in replacement
        substitute(module("org.lz4:lz4-java"))
            .using(module(libs.lz4.get().toString()))
            .because("Force safe version of LZ4 across all modules")
    }
}
// --- Vulnerability fixes end ---

kotlin {
    jvmToolchain(21)
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
            targets {
                all {
                    testTask {
                        testLogging {
                            showStandardStreams = true
                            showExceptions = true
                            showCauses = true
                            showStackTraces = true
                            exceptionFormat = TestExceptionFormat.FULL
                            events("skipped", "failed")
                        }
                    }
                }
            }
        }
        register<JvmTestSuite>("integrationTest2") {
            description =
                "Run integration tests backed by a Testcontainers-managed Kafka broker (located in src/integrationTest2/...)."
            useJUnitJupiter()
            dependencies {
                implementation(project())
                implementation(libs.radar.schemas.commons)
                implementation(libs.testcontainers.kafka)
                implementation(libs.testcontainers.junit.jupiter)
            }
            targets {
                all {
                    testTask {
                        shouldRunAfter(test)
                        testLogging {
                            showStandardStreams = true
                            showExceptions = true
                            showCauses = true
                            showStackTraces = true
                            exceptionFormat = TestExceptionFormat.FULL
                            events("passed", "skipped", "failed")
                        }
                    }
                }
            }
        }
    }
}

configurations["integrationTest2Implementation"].extendsFrom(configurations.testImplementation.get())
repositories {
    mavenCentral()
}
