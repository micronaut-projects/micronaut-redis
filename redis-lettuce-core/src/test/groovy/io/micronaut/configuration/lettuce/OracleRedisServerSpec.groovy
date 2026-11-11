package io.micronaut.configuration.lettuce

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisCommandTimeoutException
import io.lettuce.core.RedisFuture
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.protocol.ProtocolVersion
import io.micronaut.context.ApplicationContext
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.MountableFile
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.ExecutionException

class OracleRedisServerSpec extends Specification {

    @Shared
    @AutoCleanup
    GenericContainer<?> oracle = new GenericContainer<>("gvenzl/oracle-free:23.26.3-faststart")
        .withEnv("ORACLE_PASSWORD", "test")
        .withEnv("APP_USER", "test")
        .withEnv("APP_USER_PASSWORD", "test")
        .withCopyFileToContainer(MountableFile.forClasspathResource("oracle/redis-server.sql"), "/container-entrypoint-startdb.d/redis-server.sql")
        .withExposedPorts(1521, 6379)
        .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE.*", 1).withStartupTimeout(Duration.ofMinutes(5)))

    def setupSpec() {
        oracle.start()
    }

    void "test reading rows through the Oracle Redis server over RESP2"() {
        given:
        ApplicationContext context = ApplicationContext.run(
            "redis.host": oracle.host,
            "redis.port": oracle.getMappedPort(6379),
            "redis.username": "test",
            "redis.password": "test",
            "redis.protocol-version": "RESP2",
            "redis.library-name": "",
            "redis.library-version": ""
        )
        StatefulRedisConnection<String, String> connection = context.getBean(StatefulRedisConnection)

        expect:
        context.getBean(RedisClient).options.protocolVersion == ProtocolVersion.RESP2
        connection.sync().get("test.book:id=1") == "1:The Stand:1153"
        connection.sync().hget("test.book:id=2", "TITLE") == "Dune"
        connection.sync().hgetall("test.book:id=2") == [ID: "2", TITLE: "Dune", PAGES: "412"]
        connection.sync().mget("test.book:id=1", "test.book:id=2")*.value == ["1:The Stand:1153", "2:Dune:412"]

        when: "writes are accepted but not applied"
        String reply = connection.sync().set("test.book:id=1", "changed")

        then:
        reply == "OK"
        connection.sync().get("test.book:id=1") == "1:The Stand:1153"

        cleanup:
        context?.close()
    }

    void "test commands sent together are not all answered"() {
        given:
        ApplicationContext context = ApplicationContext.run(
            "redis.host": oracle.host,
            "redis.port": oracle.getMappedPort(6379),
            "redis.username": "test",
            "redis.password": "test",
            "redis.protocol-version": "RESP2",
            "redis.library-name": "",
            "redis.library-version": "",
            "redis.timeout": "2s"
        )
        StatefulRedisConnection<String, String> connection = context.getBean(StatefulRedisConnection)

        when:
        connection.setAutoFlushCommands(false)
        RedisFuture<String> first = connection.async().get("test.book:id=1")
        RedisFuture<String> second = connection.async().get("test.book:id=2")
        connection.flushCommands()
        first.get()
        second.get()

        then:
        ExecutionException e = thrown()
        e.cause instanceof RedisCommandTimeoutException

        cleanup:
        context?.close()
    }
}
