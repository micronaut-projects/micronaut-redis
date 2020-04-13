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
package io.micronaut.configuration.lettuce.cache

import io.micronaut.context.ApplicationContext
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class ExpirationSpec extends Specification {

    @Shared
    @AutoCleanup
    ApplicationContext applicationContext = ApplicationContext.run(
            'redis.type': 'embedded',
            'redis.caches.test1.enabled': 'true',
            'redis.caches.test1.expire-after-write': '1s',
            'redis.caches.test2.enabled': 'true',
            'redis.caches.test2.expiration-after-write-policy': 'io.micronaut.configuration.lettuce.cache.TestExpirationPolicy'
    )

    void "test constant-expiration-after-write-policy expires after set timeout"() {
        when:
        TimeService timeService = applicationContext.getBean(TimeService)
        long result = timeService.getTimeWithConstantExpirationPolicy()

        then:
        timeService.getTimeWithConstantExpirationPolicy() == result

        when:
        Thread.sleep(1000)

        then:
        timeService.getTimeWithConstantExpirationPolicy() != result
    }

    void "test dynamic-expiration-after-write-policy expires after set timeout"() {
        when:
        TimeService timeService = applicationContext.getBean(TimeService)
        long result = timeService.getTimeWithDynamicExpirationPolicy()

        then:
        timeService.getTimeWithDynamicExpirationPolicy() == result

        when:
        Thread.sleep(1500)

        then:
        timeService.getTimeWithDynamicExpirationPolicy() != result
    }


}
