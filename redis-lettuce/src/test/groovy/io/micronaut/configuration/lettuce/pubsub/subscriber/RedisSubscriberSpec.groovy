package io.micronaut.configuration.lettuce.pubsub.subscriber

import io.micronaut.configuration.lettuce.pubsub.PubSubBaseSpec

class RedisSubscriberSpec extends PubSubBaseSpec {

    MessageChannelSubscriber target

    def setup() {
        target = getBean(MessageChannelSubscriber)
    }

    def "Message handling test" () {
        when:
        publishDummyMessage()

        then:
        target.log.size() == 1
    }

}
