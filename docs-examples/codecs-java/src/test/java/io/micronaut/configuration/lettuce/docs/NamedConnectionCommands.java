package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.api.StatefulRedisConnection;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Requires(property = "spec.name", value = "NamedConnectionTest")
@Singleton
public final class NamedConnectionCommands {

    // tag::namedConnection[]
    @Inject @Named("foo") StatefulRedisConnection<String, String> connection;
    // end::namedConnection[]

    void commands() {
        connection.sync().set("named", "foo");
    }
}
