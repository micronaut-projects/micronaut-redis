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
        def port = SocketUtils.findAvailableTcpPort()
        RedisServer redisServer = RedisServer.builder().port(port).setting(MAX_HEAP_SETTING).build()
        redisServer.start()

        when:
        ApplicationContext applicationContext = ApplicationContext.run('redis.uri':"redis://localhost:$port")
        StatefulRedisConnection client = applicationContext.getBean(StatefulRedisConnection)
        def command = client.sync()
        then:
        command.set("foo", "bar")
        command.get("foo") == "bar"

        cleanup:
        redisServer.stop()
        applicationContext.stop()
    }

    void "test multi redis server config by URI"() {
        given:
        def port = SocketUtils.findAvailableTcpPort()
        RedisServer redisServer = RedisServer.builder().port(port).setting(MAX_HEAP_SETTING).build()
        redisServer.start()

        ApplicationContext applicationContext = ApplicationContext.run(['redis.servers.foo.uri':"redis://localhost:$port",'redis.servers.bar.uri':"redis://localhost:$port"])
        when:
        RedisClient clientFoo = applicationContext.getBean(RedisClient, Qualifiers.byName("foo"))
        def innerRedisURI = clientFoo.@redisURI
        def commandFoo = clientFoo.connect().sync()
        then:
        innerRedisURI.port == port
        commandFoo.info().contains("tcp_port:$port")
        commandFoo.set("foo", "bar")
        commandFoo.get("foo") == "bar"

        when:
        StatefulRedisConnection clientBar = applicationContext.getBean(StatefulRedisConnection, Qualifiers.byName("bar"))
        def commandBar = clientBar.sync()
        then:
        commandBar.info().contains("tcp_port:$port")

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
}
