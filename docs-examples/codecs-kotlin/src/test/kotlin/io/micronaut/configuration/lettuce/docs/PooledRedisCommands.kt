package io.micronaut.configuration.lettuce.docs

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.support.AsyncPool
import io.micronaut.context.annotation.Requires
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.util.concurrent.TimeUnit

@Requires(property = "redis.pool.enabled", value = "true")
@Singleton
class PooledRedisCommands {

    // tag::pool[]
    @Inject
    lateinit var pool: AsyncPool<StatefulRedisConnection<String, String>>

    fun commands() {
        val connection = pool.acquire().get(5, TimeUnit.SECONDS)
        try {
            connection.sync().set("first", "one")
        } finally {
            pool.release(connection).get(5, TimeUnit.SECONDS)
        }
    }
    // end::pool[]
}
