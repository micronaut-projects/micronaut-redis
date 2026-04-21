/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.lettuce;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.annotation.Internal;
import org.jspecify.annotations.NonNull;
import io.micronaut.inject.qualifiers.Qualifiers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Internal utility methods for configuration.
 *
 * @author Graeme Rocher, Illia Kovalov
 * @since 1.0
 */
@Internal
public class RedisConnectionUtil {
    /**
     * Utility method for establishing a redis connection.
     *
     * @param beanLocator  The bean locator to use
     * @param serverName   The server name to use
     * @param errorMessage The error message to use if the connection can't be found
     * @return The connection
     * @throws ConfigurationException If the connection cannot be found
     */
    public static @NonNull AbstractRedisClient findClient(BeanLocator beanLocator, Optional<String> serverName, String errorMessage) {
        Optional<? extends AbstractRedisClient> clusterConn = findRedisClusterClient(beanLocator, serverName);
        if (clusterConn.isPresent()) {
            return clusterConn.get();
        }
        Optional<? extends AbstractRedisClient> conn = findRedisClient(beanLocator, serverName);
        if (conn.isPresent()) {
            return conn.get();
        }
        throw new ConfigurationException(errorMessage);
    }

    /**
     * Utility method for establishing a redis connection.
     *
     * @param beanLocator  The bean locator to use
     * @param serverName   The server name to use
     * @param errorMessage The error message to use if the connection can't be found
     * @return The connection
     * @throws ConfigurationException If the connection cannot be found
     */
    public static StatefulConnection findRedisConnection(BeanLocator beanLocator, Optional<String> serverName, String errorMessage) {
        Optional<StatefulRedisClusterConnection> clusterConn = findStatefulRedisClusterConnection(beanLocator, serverName);
        if (clusterConn.isPresent()) {
            return clusterConn.get();
        }
        Optional<StatefulRedisConnection> conn = findStatefulRedisConnection(beanLocator, serverName);
        if (conn.isPresent()) {
            return conn.get();
        }
        throw new ConfigurationException(errorMessage);
    }

    /**
     * Utility method to find a named bean first and fallback to the default bean.
     *
     * @param beanLocator Bean locator
     * @param beanType Bean type
     * @param serverName Server name
     * @param <T> Bean type
     * @return The matching bean if available
     */
    public static <T> Optional<T> findNamedOrDefaultBean(BeanLocator beanLocator, Class<T> beanType, Optional<String> serverName) {
        Optional<T> namedConn = serverName.flatMap(name -> beanLocator.findBean(beanType, Qualifiers.byName(name)));
        if (namedConn.isPresent()) {
            return namedConn;
        }
        return beanLocator.findBean(beanType);
    }

    private static Optional<StatefulRedisClusterConnection> findStatefulRedisClusterConnection(BeanLocator beanLocator, Optional<String> serverName) {
        return findNamedOrDefaultBean(beanLocator, StatefulRedisClusterConnection.class, serverName);
    }

    private static Optional<StatefulRedisConnection> findStatefulRedisConnection(BeanLocator beanLocator, Optional<String> serverName) {
        return findNamedOrDefaultBean(beanLocator, StatefulRedisConnection.class, serverName);
    }

    /**
     * Utility method for opening a new bytes redis connection.
     *
     * @param beanLocator  The bean locator to use
     * @param serverName   The server name to use
     * @param errorMessage The error message to use if the connection can't be found
     * @return The connection
     * @throws ConfigurationException If the connection cannot be found
     */
    public static StatefulConnection<byte[], byte[]> openBytesRedisConnection(BeanLocator beanLocator, Optional<String> serverName, String errorMessage) {
        Optional<DefaultRedisConfiguration> config = beanLocator.findBean(DefaultRedisConfiguration.class);
        Optional<RedisClusterClient> redisClusterClient = findRedisClusterClient(beanLocator, serverName);
        if (redisClusterClient.isPresent()) {
            StatefulRedisClusterConnection<byte[], byte[]> conn = redisClusterClient.get().connect(ByteArrayCodec.INSTANCE);
            if (config.isPresent() && config.get().getReadFrom().isPresent()) {
                conn.setReadFrom(config.get().getReadFrom().get());
            }
            return conn;
        }
        Optional<RedisClient> redisClient = findRedisClient(beanLocator, serverName);
        if (redisClient.isPresent()) {
            if (config.isPresent() && config.get().getUri().isPresent() && !config.get().getReplicaUris().isEmpty()) {
                List<RedisURI> uris = new ArrayList<>(config.get().getReplicaUris());
                uris.add(config.get().getUri().get());

                StatefulRedisMasterReplicaConnection<byte[], byte[]> connection = MasterReplica.connect(
                    redisClient.get(),
                    ByteArrayCodec.INSTANCE,
                    uris
                );
                if (config.get().getReadFrom().isPresent()) {
                    connection.setReadFrom(config.get().getReadFrom().get());
                }

                return connection;
            } else {
                return redisClient.get().connect(ByteArrayCodec.INSTANCE);
            }
        }
        throw new ConfigurationException(errorMessage);
    }

    private static Optional<RedisClusterClient> findRedisClusterClient(BeanLocator beanLocator, Optional<String> serverName) {
        return findNamedOrDefaultBean(beanLocator, RedisClusterClient.class, serverName);
    }

    private static Optional<RedisClient> findRedisClient(BeanLocator beanLocator, Optional<String> serverName) {
        return findNamedOrDefaultBean(beanLocator, RedisClient.class, serverName);
    }

}
