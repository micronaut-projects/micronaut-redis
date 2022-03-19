package io.micronaut.configuration.lettuce.pubsub

import io.micronaut.configuration.lettuce.pubsub.processor.RedisPubSub
import spock.lang.Ignore

class RedisPubSubSpec extends PubSubBaseSpec {

    RedisPubSub target

    def setup() {
        target = newPubSubInstance()
    }

    @Ignore
    def "Test simple subscriber" () {
        given:
        boolean sense = false
        RedisSubscriber subscriber = new RedisSubscriber({ sense = true })

        when:
        target.subscribe(DEFAULT_CHANNEL, subscriber)

        and:
        publish(DUMMY_MESSAGE, DEFAULT_CHANNEL)

        then:
        assert sense
    }

}
