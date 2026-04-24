package io.micronaut.configuration.lettuce

import groovy.transform.CompileStatic
import io.micronaut.redis.test.RedisContainerUtils
import spock.lang.Specification

@CompileStatic
abstract class RedisSpec extends Specification {

    def cleanupSpec() {
        RedisContainerUtils.close();
    }
}
