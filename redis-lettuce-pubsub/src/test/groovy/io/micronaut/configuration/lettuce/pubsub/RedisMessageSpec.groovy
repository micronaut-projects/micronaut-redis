package io.micronaut.configuration.lettuce.pubsub

import spock.lang.Specification

class RedisMessageSpec extends Specification {

    void "test body accessors return defensive copies"() {
        given:
        byte[] original = "Dune".bytes
        def message = new RedisMessage(original, "books.created", null)

        when:
        original[0] = (byte) 'X'
        byte[] recordBody = message.body()
        byte[] getterBody = message.getBody()
        recordBody[1] = (byte) 'Y'
        getterBody[2] = (byte) 'Z'

        then:
        new String(message.body()) == "Dune"
        new String(message.getBody()) == "Dune"
    }
}
