/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.lettuce

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.micronaut.context.ApplicationContext
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.inject.qualifiers.Qualifiers
import redis.embedded.RedisServer
import spock.lang.Specification

/**
 * @author Graeme Rocher
 * @since 1.0
 */
class RedisClientFactorySpec extends Specification{


    private static final String MAX_HEAP_SETTING = "maxmemory 256M"

    void "test redis server config by port"() {
        given:
        def port = SocketUtils.findAvailableTcpPort()
        RedisServer redisServer = new RedisServer(port).builder().port(port).setting(MAX_HEAP_SETTING).build()
        redisServer.start()

        when:
        ApplicationContext applicationContext = ApplicationContext.run('redis.port':port)
        StatefulRedisConnection connection = applicationContext.getBean(StatefulRedisConnection)

        then:
        // tag::commands[]
        RedisCommands<String, String> commands = connection.sync()
        commands.set("foo", "bar")
        commands.get("foo") == "bar"
        // end::commands[]

        cleanup:
        redisServer.stop()
        applicationContext.stop()
    }

    void "test redis server config by URI"() {
        given:
        int port = SocketUtils.findAvailableTcpPort()
        RedisServer redisServer = RedisServer.builder().port(port).setting(MAX_HEAP_SETTING).build()
        redisServer.start()

        when:
        ApplicationContext applicationContext = ApplicationContext.run('redis.uri':"redis://localhost:$port")
        StatefulRedisConnection client = applicationContext.getBean(StatefulRedisConnection)
        RedisCommands<?,?> command = client.sync()
        then:
        command.set("foo", "bar")
        command.get("foo") == "bar"

        cleanup:
        redisServer.stop()
        applicationContext.stop()
    }

    void "test multi redis server config by URI"() {
        given:
        int port = SocketUtils.findAvailableTcpPort()
        RedisServer redisServer = RedisServer.builder().port(port).setting(MAX_HEAP_SETTING).build()
        redisServer.start()

        ApplicationContext applicationContext = ApplicationContext.run(['redis.servers.foo.uri':"redis://localhost:$port",
                                                                        'redis.servers.foo.client-name':"foo-client-name",
                                                                        'redis.servers.bar.uri':"redis://localhost:$port"])
        when:
        RedisClient clientFoo = applicationContext.getBean(RedisClient, Qualifiers.byName("foo"))
        RedisURI innerRedisURI = clientFoo.@redisURI
        RedisCommands<?,?> commandFoo = clientFoo.connect().sync()

        then:
        innerRedisURI.port == port
        innerRedisURI.clientName == "foo-client-name"
        commandFoo.info().contains("tcp_port:$port")
        commandFoo.set("foo", "bar")
        commandFoo.get("foo") == "bar"

        when:
        RedisClient clientBar = applicationContext.getBean(RedisClient, Qualifiers.byName("bar"))
        RedisURI innerBarRedisURI = clientBar.@redisURI
        RedisCommands<?,?> commandBar = clientBar.connect().sync()
        then:
        commandBar.info().contains("tcp_port:$port")
        !innerBarRedisURI.clientName

        cleanup:
        redisServer.stop()
        applicationContext.stop()
    }

    void "test redis thread pool settings"() {
        given:
        def port = SocketUtils.findAvailableTcpPort()
        RedisServer redisServer = RedisServer.builder().port(port).setting(MAX_HEAP_SETTING).build()
        redisServer.start()

        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uri':"redis://localhost:$port",
                'redis.io-thread-pool-size':10,
                'redis.computation-thread-pool-size':20,
        ])
        when:
        RedisClient client = applicationContext.getBean(RedisClient)

        then:
        client.getResources().computationThreadPoolSize() == 20
        client.getResources().ioThreadPoolSize() == 10

        cleanup:
        redisServer.stop()
        applicationContext.stop()
    }

    void "test redis client name settings"() {
        given:
        int port = SocketUtils.findAvailableTcpPort()
        RedisServer redisServer = RedisServer.builder().port(port).setting(MAX_HEAP_SETTING).build()
        redisServer.start()

        ApplicationContext applicationContext = ApplicationContext.run([
                'redis.uri':"redis://localhost:$port",
                'redis.client-name':"test-name"
        ])
        when:
        RedisClient client = applicationContext.getBean(RedisClient)
        RedisURI innerRedisURI = client.@redisURI

        then:
        innerRedisURI.clientName == "test-name"

        cleanup:
        redisServer.stop()
        applicationContext.stop()
    }
}
