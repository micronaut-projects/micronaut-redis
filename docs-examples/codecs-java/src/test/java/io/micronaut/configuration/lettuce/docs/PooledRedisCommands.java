package io.micronaut.configuration.lettuce.docs;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.support.AsyncPool;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.concurrent.TimeUnit;

@Requires(property = "redis.pool.enabled", value = "true")
@Singleton
public final class PooledRedisCommands {

    // tag::pool[]
    @Inject AsyncPool<StatefulRedisConnection<String, String>> pool;

    void commands() throws Exception {
        StatefulRedisConnection<String, String> connection = pool.acquire().get(5, TimeUnit.SECONDS);
        try {
            connection.sync().set("first", "one");
        } finally {
            pool.release(connection).get(5, TimeUnit.SECONDS);
        }
    }
    // end::pool[]
}
