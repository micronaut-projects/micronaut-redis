plugins {
    id("io.micronaut.build.internal.java-base")
    `java-library`
}
dependencies {
    implementation(platform(mnTest.boms.testcontainers))
    api(libs.testcontainers)
    api(libs.testcontainers.redis)
}
