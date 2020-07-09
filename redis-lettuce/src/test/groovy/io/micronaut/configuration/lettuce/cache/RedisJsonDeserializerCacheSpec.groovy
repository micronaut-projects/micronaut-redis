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

/**
 * @author Denis Stepanov
 */
class RedisJsonDeserializerCacheSpec extends RedisCacheSpec {

    ApplicationContext createApplicationContext() {
        ApplicationContext.run(
                'redis.type': 'embedded',
                'redis.caches.test.enabled': 'true',
                'redis.caches.test.valueSerializer': 'io.micronaut.jackson.serialize.JacksonObjectSerializer'
        )
    }
}
