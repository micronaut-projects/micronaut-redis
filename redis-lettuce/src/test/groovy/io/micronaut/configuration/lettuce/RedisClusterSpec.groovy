package io.micronaut.configuration.lettuce

import groovy.transform.CompileStatic
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.models.partitions.RedisClusterNode
import io.micronaut.redis.test.RedisClusterContainerUtils
import spock.lang.Specification

@CompileStatic
abstract class RedisClusterSpec extends Specification {

    def cleanupSpec() {
        RedisClusterContainerUtils.close()
    }

    List<String> getRedisClusterUris() {
        return RedisClusterContainerUtils.getRedisClusterUris()
    }

    RedisClusterClient fixPartitions(RedisClusterClient client) {
        client.partitions.stream().map(RedisClusterNode::getUri).forEach(RedisClusterContainerUtils::fixRedisURI)
        return client;
    }
}
