package io.micronaut.configuration.lettuce

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.cluster.models.partitions.RedisClusterNode
import io.micronaut.redis.test.RedisClusterContainerUtils
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@CompileStatic
abstract class RedisClusterSpec extends Specification {

    def cleanupSpec() {
        RedisClusterContainerUtils.close()
    }

    List<String> getRedisClusterUris() {
        return RedisClusterContainerUtils.getRedisClusterUris()
    }

    @CompileDynamic
    RedisClusterClient fixPartitions(RedisClusterClient client) {
        def conditions = new PollingConditions(timeout: 30, initialDelay: 1, delay: 0.5)
        conditions.eventually {
            client.refreshPartitions()
            client.partitions.stream().map(RedisClusterNode::getUri).forEach(RedisClusterContainerUtils::fixRedisURI)

            StatefulRedisClusterConnection<String, String> connection = client.connect(StringCodec.UTF8)
            try {
                RedisAdvancedClusterCommands<String, String> commands = connection.sync()
                String key = 'cluster-ready'
                commands.set(key, 'ok')
                assert commands.get(key) == 'ok'
            } finally {
                connection.close()
            }
        }
        return client;
    }
}
