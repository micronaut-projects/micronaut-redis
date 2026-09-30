package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.context.annotation.Requires
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton

@Requires(property = "spec.name", value = "NamedConnectionTest")
@Singleton
class NamedConnectionCommands {

    // tag::namedConnection[]
    @Inject
    @field:Named("foo")
    lateinit var connection: StatefulRedisConnection<String, String>
    // end::namedConnection[]

    fun commands() {
        connection.sync().set("named", "foo")
    }
}
