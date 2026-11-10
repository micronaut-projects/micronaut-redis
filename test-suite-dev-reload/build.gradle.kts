plugins {
    id("io.micronaut.build.internal.java-base")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

dependencies {
    testImplementation(platform(mn.micronaut.core.bom))
    testImplementation(project(":micronaut-redis-lettuce-core"))
    testImplementation(project(":micronaut-redis-lettuce-pubsub"))
    testImplementation(project(":micronaut-redis-lettuce-cache"))
    testImplementation(mnSerde.micronaut.serde.jackson)
    testImplementation(mn.micronaut.dev.tck)
    // the reload harness compiles the application under test with the processors on the test classpath
    testImplementation(mn.micronaut.inject.java)
    testImplementation(project(":test-suite-utils"))
    testImplementation(platform(mnTest.boms.testcontainers))
    testImplementation(mnTest.junit.jupiter.api)
    testRuntimeOnly(mnTest.junit.jupiter.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(mnLogging.logback.classic)
}
