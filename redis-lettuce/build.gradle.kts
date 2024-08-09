plugins {
    id("io.micronaut.build.internal.redis-module")
}

dependencies {
    annotationProcessor(mn.micronaut.graal)

    api(libs.managed.lettuce)
    api(mnCache.micronaut.cache.core)

    compileOnly(mnSession.micronaut.session)
    compileOnly(mn.micronaut.management)
    compileOnly(mnMicrometer.micronaut.micrometer.core)
    compileOnly(libs.redis.embedded)

    testAnnotationProcessor(mn.micronaut.inject.java)

    testImplementation(mnTestResources.testcontainers.core)

    testImplementation(mn.reactor)
    testImplementation(libs.jupiter.api)
    testImplementation(mnTest.micronaut.test.junit5)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(mn.micronaut.inject.java)
    testImplementation(mn.micronaut.management)
    testImplementation(mnMicrometer.micronaut.micrometer.core)
    testImplementation(mnSerde.micronaut.serde.jackson)
    testImplementation(mn.micronaut.jackson.databind)
    testImplementation(mn.micronaut.function.web)

    testImplementation(mnSession.micronaut.session)
    testRuntimeOnly(mn.micronaut.http.server.netty)
    testImplementation(mn.micronaut.http.client)
    testImplementation(libs.redis.embedded)
    testRuntimeOnly(mnLogging.logback.classic)
    testImplementation(mn.snakeyaml)
}
