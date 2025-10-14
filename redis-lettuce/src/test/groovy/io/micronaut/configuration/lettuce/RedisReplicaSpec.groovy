package io.micronaut.configuration.lettuce

import groovy.transform.CompileStatic
import io.micronaut.redis.test.RedisReplicaContainerUtils
import spock.lang.Specification

@CompileStatic
abstract class RedisReplicaSpec extends Specification {

    def cleanupSpec() {
        RedisReplicaContainerUtils.close();
    }

    List<String> getRedisReplicaUris() {
        return RedisReplicaContainerUtils.getRedisReplicaUris()
    }

    String getPrimaryUri() {
        return RedisReplicaContainerUtils.getRedisPrimaryUri()
    }
}
