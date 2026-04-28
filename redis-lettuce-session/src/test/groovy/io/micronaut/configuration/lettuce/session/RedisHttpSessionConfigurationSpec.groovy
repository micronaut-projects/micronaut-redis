package io.micronaut.configuration.lettuce.session

import spock.lang.Specification

class RedisHttpSessionConfigurationSpec extends Specification {

    void "namespace updates derived session defaults"() {
        given:
        def configuration = new RedisHttpSessionConfiguration()

        when:
        configuration.setNamespace("myapp:sessions:")

        then:
        configuration.namespace == "myapp:sessions:"
        configuration.activeSessionsKey == "myapp:sessions:active-sessions"
        configuration.sessionCreatedTopic == "myapp:sessions:event:session-created"
    }

    void "explicit session keys are preserved across namespace changes"() {
        given:
        def configuration = new RedisHttpSessionConfiguration()

        when:
        configuration.setActiveSessionsKey("shared:active-sessions")
        configuration.setSessionCreatedTopic("shared:event:session-created")
        configuration.setNamespace("myapp:sessions:")

        then:
        configuration.activeSessionsKey == "shared:active-sessions"
        configuration.sessionCreatedTopic == "shared:event:session-created"
    }
}
