/*
 * Copyright 2017-2026 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.lettuce;

import io.lettuce.core.RedisCredentialsProvider;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base configuration that keeps track of explicitly bound URI properties so they can be
 * applied even when {@code redis.uri} is configured as a single URI string.
 */
abstract class RedisConfigurationWithUriSettings extends AbstractRedisConfiguration {

    private final AtomicReference<ConfiguredRedisUriSettings> configuredRedisUriSettings =
        new AtomicReference<>(ConfiguredRedisUriSettings.EMPTY);

    @Override
    public Optional<RedisURI> getUri() {
        return super.getUri().map(this::applyConfiguredRedisUriSettings);
    }

    @Override
    public List<RedisURI> getUris() {
        return super.getUris().stream().map(this::applyConfiguredRedisUriSettings).toList();
    }

    @Override
    public List<RedisURI> getReplicaUris() {
        return super.getReplicaUris().stream().map(this::applyConfiguredRedisUriSettings).toList();
    }

    @Override
    public void setTimeout(Duration timeout) {
        super.setTimeout(timeout);
        configuredRedisUriSettings.updateAndGet(settings -> settings.withTimeout(timeout));
    }

    @Override
    public void setDatabase(int database) {
        super.setDatabase(database);
        configuredRedisUriSettings.updateAndGet(settings -> settings.withDatabase(database));
    }

    @Override
    public void setSsl(boolean ssl) {
        super.setSsl(ssl);
        configuredRedisUriSettings.updateAndGet(settings -> settings.withSsl(ssl));
    }

    @Override
    public void setStartTls(boolean startTls) {
        super.setStartTls(startTls);
        configuredRedisUriSettings.updateAndGet(settings -> settings.withStartTls(startTls));
    }

    @Override
    public void setVerifyPeer(boolean verifyPeer) {
        super.setVerifyPeer(verifyPeer);
        configuredRedisUriSettings.updateAndGet(settings -> settings.withVerifyMode(getVerifyMode()));
    }

    @Override
    public void setVerifyPeer(SslVerifyMode verifyMode) {
        super.setVerifyPeer(verifyMode);
        configuredRedisUriSettings.updateAndGet(settings -> settings.withVerifyMode(verifyMode));
    }

    @Override
    public void setAuthentication(CharSequence password) {
        super.setAuthentication(password);
        updateConfiguredCredentialsProvider(getCredentialsProvider());
    }

    @Override
    public void setAuthentication(char[] password) {
        super.setAuthentication(password);
        updateConfiguredCredentialsProvider(getCredentialsProvider());
    }

    @Override
    public void setAuthentication(String username, char[] password) {
        super.setAuthentication(username, password);
        updateConfiguredCredentialsProvider(getCredentialsProvider());
    }

    @Override
    public void setAuthentication(String username, CharSequence password) {
        super.setAuthentication(username, password);
        updateConfiguredCredentialsProvider(getCredentialsProvider());
    }

    @Override
    public void setCredentialsProvider(RedisCredentialsProvider credentialsProvider) {
        super.setCredentialsProvider(credentialsProvider);
        updateConfiguredCredentialsProvider(credentialsProvider);
    }

    private void updateConfiguredCredentialsProvider(RedisCredentialsProvider credentialsProvider) {
        configuredRedisUriSettings.updateAndGet(settings -> settings.withCredentialsProvider(credentialsProvider));
    }

    private RedisURI applyConfiguredRedisUriSettings(RedisURI redisURI) {
        return configuredRedisUriSettings.get().apply(redisURI, getClientName());
    }

    private record ConfiguredRedisUriSettings(
        Duration timeout,
        Integer database,
        Boolean ssl,
        Boolean startTls,
        SslVerifyMode verifyMode,
        RedisCredentialsProvider credentialsProvider
    ) {
        private static final ConfiguredRedisUriSettings EMPTY = new ConfiguredRedisUriSettings(null, null, null, null, null, null);

        private ConfiguredRedisUriSettings withTimeout(Duration newTimeout) {
            return new ConfiguredRedisUriSettings(newTimeout, database, ssl, startTls, verifyMode, credentialsProvider);
        }

        private ConfiguredRedisUriSettings withDatabase(Integer newDatabase) {
            return new ConfiguredRedisUriSettings(timeout, newDatabase, ssl, startTls, verifyMode, credentialsProvider);
        }

        private ConfiguredRedisUriSettings withSsl(Boolean newSsl) {
            return new ConfiguredRedisUriSettings(timeout, database, newSsl, startTls, verifyMode, credentialsProvider);
        }

        private ConfiguredRedisUriSettings withStartTls(Boolean newStartTls) {
            return new ConfiguredRedisUriSettings(timeout, database, ssl, newStartTls, verifyMode, credentialsProvider);
        }

        private ConfiguredRedisUriSettings withVerifyMode(SslVerifyMode newVerifyMode) {
            return new ConfiguredRedisUriSettings(timeout, database, ssl, startTls, newVerifyMode, credentialsProvider);
        }

        private ConfiguredRedisUriSettings withCredentialsProvider(RedisCredentialsProvider newCredentialsProvider) {
            return new ConfiguredRedisUriSettings(timeout, database, ssl, startTls, verifyMode, newCredentialsProvider);
        }

        private RedisURI apply(RedisURI redisURI, String clientName) {
            RedisURI.Builder builder = RedisURI.builder(redisURI);
            if (credentialsProvider != null) {
                builder.withAuthentication(credentialsProvider);
            }
            if (timeout != null) {
                builder.withTimeout(timeout);
            }
            if (database != null) {
                builder.withDatabase(database);
            }
            if (clientName != null) {
                builder.withClientName(clientName);
            }
            if (ssl != null) {
                builder.withSsl(ssl);
            }
            if (startTls != null) {
                builder.withStartTls(startTls);
            }
            if (verifyMode != null) {
                builder.withVerifyPeer(verifyMode);
            }
            return builder.build();
        }
    }
}
