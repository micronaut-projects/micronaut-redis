/*
 * Copyright 2017-2020 original authors
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
package io.micronaut.configuration.lettuce.session;

import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.api.async.RedisSortedSetAsyncCommands;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubAsyncCommandsImpl;
import io.lettuce.core.pubsub.RedisPubSubReactiveCommandsImpl;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.micronaut.configuration.lettuce.RedisConnectionUtil;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.annotation.TypeHint;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.convert.value.MutableConvertibleValues;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.session.InMemorySession;
import io.micronaut.session.InMemorySessionStore;
import io.micronaut.session.Session;
import io.micronaut.session.SessionIdGenerator;
import io.micronaut.session.SessionSettings;
import io.micronaut.session.SessionStore;
import io.micronaut.session.event.SessionCreatedEvent;
import io.micronaut.session.event.SessionDeletedEvent;
import io.micronaut.session.event.SessionExpiredEvent;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.micronaut.configuration.lettuce.session.RedisSessionStore.RedisSession.ATTR_CREATION_TIME;
import static io.micronaut.configuration.lettuce.session.RedisSessionStore.RedisSession.ATTR_LAST_ACCESSED;
import static io.micronaut.configuration.lettuce.session.RedisSessionStore.RedisSession.ATTR_MAX_INACTIVE_INTERVAL;

/**
 * <p>An implementation of the {@link SessionStore} interface for Redis. Partially inspired by Spring Session.</p>
 *
 * <h2>Session serialization</h2>
 *
 * <p>Sessions are stored within Redis hashes. The values contained within the sessions are serialized by the {@link ObjectSerializer} configured by {@link RedisHttpSessionConfiguration#getValueSerializer()}
 * which by default uses Java serialization. The Jackson Micronaut module includes the ability the configure JSON serialization as an alternative.</p>
 *
 * <h2>Storage Details</h2>
 *
 * <p>Sessions are stored within Redis hashes by default prefixed with {@code micronaut:session:sessions:[SESSION_ID]}. The expiry of the hash is set to 5 minutes after the actual expiry and
 * expired sessions are simply not returned by {@link #findSession(String)}</p>
 *
 * <p>More exact session expiry entries are stored with keys {@code micronaut:session:expiry:[SESSION_ID]} and current active sessions are tracked within sorted set at the key {@code micronaut:session:active-sessions}.
 * The entries within the set are sorted by expiry time and a scheduled job that runs every minute periodically touches the keys within the set that match the last minute thus ensuring Redis propagates expiry events in a timely manner.</p>
 *
 * <h2>Redis Pub/Sub</h2>
 *
 * <p>This implementation requires the Redis instance to have keyspace event notification enabled with {@code notify-keyspace-events Egx}. The implementation will attempt to enable this programmatically. This behaviour can be disabled with {@link RedisHttpSessionConfiguration#isEnableKeyspaceEvents()} </p>
 *
 * @author Graeme Rocher
 * @since 1.0
 */
@Singleton
@Primary
@Requires(property = RedisSessionStore.REDIS_SESSION_ENABLED, value = StringUtils.TRUE)
@Replaces(InMemorySessionStore.class)
@TypeHint(value = {RedisPubSubAsyncCommandsImpl.class, RedisPubSubReactiveCommandsImpl.class}, accessType = TypeHint.AccessType.ALL_PUBLIC)
public class RedisSessionStore extends RedisPubSubAdapter<String, String> implements SessionStore<RedisSessionStore.RedisSession>, AutoCloseable {

    public static final String REDIS_SESSION_ENABLED = SessionSettings.HTTP + ".redis.enabled";
    private static final int EXPIRATION_SECONDS = 5;
    private static final Logger LOG  = LoggerFactory.getLogger(RedisSessionStore.class);
    private final RedisHttpSessionConfiguration sessionConfiguration;
    private final SessionIdGenerator sessionIdGenerator;
    private final ConversionService conversionService;
    private final ApplicationEventPublisher eventPublisher;
    private final ObjectSerializer valueSerializer;
    private final Charset charset;
    private final String expiryPrefix;
    private final byte[] sessionCreatedTopic;
    private final byte[] activeSessionsSet;
    private final RedisHttpSessionConfiguration.WriteMode writeMode;
    private final StatefulConnection<byte[], byte[]> connection;
    private final BaseRedisAsyncCommands<byte[], byte[]> baseRedisAsyncCommands;
    private final RedisServerCommands<byte[], byte[]> redisServerCommands;
    private final RedisSortedSetAsyncCommands<byte[], byte[]> redisSortedSetAsyncCommands;
    private final RedisStringAsyncCommands<byte[], byte[]> redisStringAsyncCommands;
    private final RedisHashAsyncCommands<byte[], byte[]> redisHashAsyncCommands;
    private final RedisKeyAsyncCommands<byte[], byte[]> redisKeyAsyncCommands;

    /**
     * Constructor.
     * @param sessionIdGenerator sessionIdGenerator
     * @param sessionConfiguration sessionConfiguration
     * @param beanLocator beanLocator
     * @param defaultSerializer The default value serializer
     * @param conversionService The conversion service
     * @param scheduledExecutorService scheduledExecutorService
     * @param eventPublisher eventPublisher
     *
     * @since 6.0.0
     */
    public RedisSessionStore(
            SessionIdGenerator sessionIdGenerator,
            RedisHttpSessionConfiguration sessionConfiguration,
            BeanLocator beanLocator,
            ObjectSerializer defaultSerializer,
            ConversionService conversionService,
            @Named(TaskExecutors.SCHEDULED) ExecutorService scheduledExecutorService,
            ApplicationEventPublisher eventPublisher) {
        this.writeMode = sessionConfiguration.getWriteMode();
        this.sessionIdGenerator = sessionIdGenerator;
        this.conversionService = conversionService;
        this.valueSerializer = sessionConfiguration
                .getValueSerializer()
                .flatMap(beanLocator::findOrInstantiateBean)
                .orElse(defaultSerializer);
        this.eventPublisher = eventPublisher;
        this.sessionConfiguration = sessionConfiguration;
        this.charset = sessionConfiguration.getCharset();
        StatefulRedisPubSubConnection<String, String> pubSubConnection = findRedisPubSubConnection(sessionConfiguration, beanLocator);

        this.expiryPrefix = sessionConfiguration.getNamespace() + "expiry:";
        this.sessionCreatedTopic = sessionConfiguration.getSessionCreatedTopic().getBytes(charset);
        this.activeSessionsSet = sessionConfiguration.getActiveSessionsKey().getBytes(charset);
        pubSubConnection.addListener(this);

        this.connection = RedisConnectionUtil.openBytesRedisConnection(beanLocator, sessionConfiguration.getServerName(), "No Redis server configured to store sessions");
        if (connection instanceof StatefulRedisConnection) {
            RedisCommands<byte[], byte[]> sync = ((StatefulRedisConnection<byte[], byte[]>) connection).sync();
            redisServerCommands = sync;
            RedisAsyncCommands<byte[], byte[]> async = ((StatefulRedisConnection<byte[], byte[]>) connection).async();
            baseRedisAsyncCommands = async;
            redisSortedSetAsyncCommands = async;
            redisStringAsyncCommands = async;
            redisHashAsyncCommands = async;
            redisKeyAsyncCommands = async;
        } else if (connection instanceof StatefulRedisClusterConnection) {
            RedisAdvancedClusterCommands<byte[], byte[]> sync = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).sync();
            redisServerCommands = sync;
            RedisAdvancedClusterAsyncCommands<byte[], byte[]> async = ((StatefulRedisClusterConnection<byte[], byte[]>) connection).async();
            baseRedisAsyncCommands = async;
            redisSortedSetAsyncCommands = async;
            redisStringAsyncCommands = async;
            redisHashAsyncCommands = async;
            redisKeyAsyncCommands = async;
        } else {
            throw new ConfigurationException("Invalid Redis connection");
        }

        RedisPubSubCommands<String, String> sync = pubSubConnection.sync();
        try {
            sync.psubscribe(
                    "__keyevent@*:del",
                    "__keyevent@*:expired"
            );
            sync.subscribe(sessionConfiguration.getSessionCreatedTopic());
        } catch (Exception e) {
            throw new ConfigurationException("Unable to subscribe to session topics: " + e.getMessage(), e);
        }

        if (sessionConfiguration.isEnableKeyspaceEvents()) {

            try {
                String result = this.redisServerCommands.configSet(
                        "notify-keyspace-events", "Egx"
                );
                if (!result.equalsIgnoreCase("ok")) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Failed to enable keyspace events on the Redis server. Manual configuration my be required");
                    }
                }
            } catch (Exception e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Failed to enable keyspace events on the Redis server. Manual configuration my be required", e);
                }
            }
        }
        if (scheduledExecutorService instanceof ScheduledExecutorService) {

            long checkDelayMillis = sessionConfiguration.getExpiredSessionCheck().toMillis();
            ((ScheduledExecutorService) scheduledExecutorService).scheduleAtFixedRate(
                    () -> {
                        long oneMinuteFromNow = Instant.now().plus(1, ChronoUnit.MINUTES).toEpochMilli();
                        long oneMinuteAgo = Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli();
                        redisSortedSetAsyncCommands.zrangebyscore(
                                activeSessionsSet, Range.create(Long.valueOf(oneMinuteAgo).doubleValue(), Long.valueOf(oneMinuteFromNow).doubleValue())
                        ).thenAccept((aboutToExpire) -> {
                            if (aboutToExpire != null) {
                                for (byte[] bytes : aboutToExpire) {
                                    byte[] expiryKey = getExpiryKey(new String(bytes, charset));
                                    redisStringAsyncCommands.get(expiryKey);
                                }
                            }
                        });
                    },
                    checkDelayMillis,
                    checkDelayMillis,
                    TimeUnit.MILLISECONDS
            );
        } else {
            throw new ConfigurationException("Configured scheduled executor service is not an instanceof ScheduledExecutorService");
        }
    }

    /**
     * Getter.
     * @return ObjectSerializer
     */
    public ObjectSerializer getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public void message(String channel, String message) {
        if (channel.equals(sessionConfiguration.getSessionCreatedTopic())) {
            findSessionInternal(message, false).whenComplete((optional, throwable) -> {
                if (throwable == null && optional.isPresent()) {
                    RedisSession session = optional.get();
                    eventPublisher.publishEvent(new SessionCreatedEvent(session));
                }
            });
        }
    }

    @Override
    public void message(String pattern, String channel, String message) {
        if (message.startsWith(expiryPrefix)) {
            boolean expired = pattern.endsWith(":expired");
            if (pattern.endsWith(":del") || expired) {
                String id = message.substring(expiryPrefix.length());
                redisSortedSetAsyncCommands.zrem(activeSessionsSet, id.getBytes(charset)).whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        if (LOG.isErrorEnabled()) {
                            LOG.error("Error removing session [" + id + "] from active sessions: " + throwable.getMessage(), throwable);
                        }
                    }
                });
                findSessionInternal(id, true).whenComplete((optional, throwable) -> {
                    if (throwable == null && optional.isPresent()) {
                        RedisSession session = optional.get();
                        eventPublisher.publishEvent(expired ? new SessionExpiredEvent(session) : new SessionDeletedEvent(session));
                    }
                });
            }
        }
    }

    @Override
    public RedisSession newSession() {
        return new RedisSession(
                sessionIdGenerator.generateId(),
                valueSerializer,
                sessionConfiguration.getMaxInactiveInterval());
    }

    @Override
    public CompletableFuture<Optional<RedisSession>> findSession(String id) {
        return findSessionInternal(id, false).thenApply(session -> {
            session.ifPresent(redisSession ->
                    redisSession.setLastAccessedTime(Instant.now())
            );
            return session;
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteSession(String id) {
        return findSessionInternal(id, true).thenCompose(session -> {
            if (session.isPresent()) {
                RedisSession redisSession = session.get();
                redisSession.setMaxInactiveInterval(Duration.ZERO);
                return save(redisSession).thenApply(ignore -> true);
            }
            return CompletableFuture.completedFuture(false);
        }).toCompletableFuture();
    }

    @Override
    public CompletableFuture<RedisSession> save(RedisSession session) {
        Map<byte[], byte[]> changes = session.delta(charset);
        if (changes.isEmpty()) {
            return CompletableFuture.completedFuture(session);
        } else {
            Set<String> removedKeys = session.removedKeys;
            byte[][] removedKeyBytes = removedKeys.stream().map(str -> (RedisSession.ATTR_PREFIX + str).getBytes(charset)).toArray(byte[][]::new);
            if (!removedKeys.isEmpty()) {
                byte[] sessionKey = getSessionKey(session.getId());
                return redisHashAsyncCommands.hdel(sessionKey, removedKeyBytes)
                        .thenCompose(ignore -> saveSessionDelta(session, changes))
                        .toCompletableFuture();
            }
            return saveSessionDelta(session, changes);
        }
    }

    private CompletableFuture<RedisSession> saveSessionDelta(RedisSession session, Map<byte[], byte[]> changes) {
        Duration maxInactiveInterval = session.getMaxInactiveInterval();
        long expirySeconds = maxInactiveInterval.getSeconds();
        byte[] sessionKey = getSessionKey(session.getId());
        byte[] sessionIdBytes = session.getId().getBytes(charset);
        if (expirySeconds == 0) {
            // delete the expired session
            RedisFuture<Long> deleteOp = redisKeyAsyncCommands.del(getExpiryKey(session));
            return deleteOp
                    .thenCompose(ignore -> redisHashAsyncCommands.hmset(sessionKey, changes))
                    .thenApply(ignore -> session)
                    .toCompletableFuture();
        }

        return redisHashAsyncCommands.hmset(
                sessionKey,
                changes
        ).thenCompose(ignore -> {
                try {
                    if (session.isNew()) {
                        session.clearModifications();

                        baseRedisAsyncCommands.publish(sessionCreatedTopic, sessionIdBytes).whenComplete((aLong, throwable12) -> {
                            if (throwable12 != null) {
                                if (LOG.isErrorEnabled()) {
                                    LOG.error("Error publishing session creation event: " + throwable12.getMessage(), throwable12);
                                }
                            }
                        });
                    } else {
                        session.clearModifications();
                    }
                } catch (Throwable e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Error publishing session creation event: " + e.getMessage(), e);
                    }
                }
                long fiveMinutesAfterExpires = expirySeconds + TimeUnit.MINUTES.toSeconds(EXPIRATION_SECONDS);
                byte[] expiryKey = getExpiryKey(session);
                double expireTimeScore = Long.valueOf(Instant.now().plus(expirySeconds, ChronoUnit.SECONDS).toEpochMilli()).doubleValue();

                CompletableFuture<Boolean> expireOp = redisKeyAsyncCommands.expire(sessionKey, fiveMinutesAfterExpires).toCompletableFuture();
                CompletableFuture<String> saveExpiryOp = redisStringAsyncCommands.setex(expiryKey, expirySeconds, String.valueOf(expirySeconds).getBytes()).toCompletableFuture();
                CompletableFuture<Long> saveActiveSessionOp = redisSortedSetAsyncCommands.zadd(activeSessionsSet, expireTimeScore, sessionIdBytes).toCompletableFuture();
                return CompletableFuture.allOf(expireOp, saveExpiryOp, saveActiveSessionOp).thenApply(ignore2 -> session);
            }).toCompletableFuture();
    }

    private byte[] getExpiryKey(RedisSession session) {
        String id = session.getId();
        return getExpiryKey(id);
    }

    private byte[] getExpiryKey(String id) {
        return (expiryPrefix + id).getBytes();
    }

    private CompletableFuture<Optional<RedisSession>> findSessionInternal(String id, boolean allowExpired) {
        return redisHashAsyncCommands.hgetall(getSessionKey(id)).thenApply(data -> {
            if (CollectionUtils.isNotEmpty(data)) {
                Map<String, byte[]> transformed = data.entrySet().stream().collect(
                        Collectors.toMap(
                                entry -> new String(entry.getKey(), charset),
                                Map.Entry::getValue
                        )
                );
                RedisSession session = new RedisSession(
                        id,
                        valueSerializer,
                        transformed);
                if (!session.isExpired() || allowExpired) {
                    return Optional.of(session);
                }
            }
            return Optional.<RedisSession>empty();
        }).toCompletableFuture();
    }

    private byte[] getSessionKey(String id) {
        return (sessionConfiguration.getNamespace() + "sessions:" + id).getBytes();
    }

    @SuppressWarnings("unchecked")
    private StatefulRedisPubSubConnection<String, String> findRedisPubSubConnection(RedisHttpSessionConfiguration sessionConfiguration, BeanLocator beanLocator) {
        Optional<String> serverName = sessionConfiguration.getServerName();
        return (StatefulRedisPubSubConnection<String, String>)
                serverName.map(name -> beanLocator.findBean(StatefulRedisPubSubConnection.class, Qualifiers.byName(name))
                        .map(conn -> (StatefulConnection) conn)
                        .orElse(
                                beanLocator.findBean(StatefulRedisPubSubConnection.class, Qualifiers.byName(name)).orElseThrow(() ->
                                        new ConfigurationException("No Redis server configured to store sessions")
                                )
                        )).orElseGet(() -> beanLocator.findBean(StatefulRedisPubSubConnection.class)
                        .map(conn -> (StatefulConnection) conn)
                        .orElse(
                                beanLocator.findBean(StatefulRedisPubSubConnection.class).orElseThrow(() ->
                                        new ConfigurationException("No Redis server configured to store sessions")
                                )
                        ));
    }

    private static Instant readLastAccessTimed(Map<String, byte[]> data) {
        return readInstant(data, ATTR_LAST_ACCESSED);
    }

    private static Duration readMaxInactive(Map<String, byte[]> data) {
        if (data != null) {
            byte[] value = data.get(ATTR_MAX_INACTIVE_INTERVAL);
            if (value != null) {
                try {
                    Long seconds = Long.valueOf(new String(value));
                    return Duration.ofSeconds(seconds);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
        return null;
    }

    private static Instant readCreationTime(Map<String, byte[]> data) {
        return readInstant(data, ATTR_CREATION_TIME);
    }

    private static Instant readInstant(Map<String, byte[]> data, String attr) {
        if (data != null) {
            byte[] value = data.get(attr);
            if (value != null) {
                try {
                    Long millis = Long.valueOf(new String(value));
                    return Instant.ofEpochMilli(millis);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
        return Instant.now();
    }

    @PreDestroy
    @Override
    public void close() {
        connection.close();
    }

    /**
     * Description states on how the session is modified.
     */
    enum Modification {
        CREATED,
        CLEARED,
        ADDITION,
        REMOVAL
    }

    /**
     * A new redis session that is in memory and not yet persisted.
     */
    class RedisSession extends InMemorySession implements Session {
        static final String ATTR_CREATION_TIME = "Creation-Time";
        static final String ATTR_LAST_ACCESSED = "Last-Accessed";
        static final String ATTR_MAX_INACTIVE_INTERVAL = "Max-Inactive-Interval";
        static final String ATTR_PREFIX = "attr:";
        final Set<String> removedKeys = new HashSet<>(2);
        final Set<String> modifiedKeys = new HashSet<>(2);
        private final Set<Modification> modifications = new HashSet<>();
        private final ObjectSerializer valueSerializer;

        /**
         * Construct a new Redis session not yet persisted.
         *
         * @param id The id of the session
         * @param valueSerializer The value serializer
         * @param maxInactiveInterval The initial max inactive interval
         */
        RedisSession(
                String id,
                ObjectSerializer valueSerializer,
                Duration maxInactiveInterval) {
            super(id, Instant.now(), maxInactiveInterval);
            this.valueSerializer = valueSerializer;
            this.modifications.add(Modification.CREATED);
        }

        /**
         * Construct a new Redis session from existing redis data.
         *
         * @param id The id of the session
         * @param valueSerializer valueSerializer
         * @param data The session data
         */
        RedisSession(
                String id,
                ObjectSerializer valueSerializer,
                Map<String, byte[]> data) {
            super(id, readCreationTime(data), readMaxInactive(data));
            this.valueSerializer = valueSerializer;
            this.lastAccessTime = readLastAccessTimed(data);

            for (String name: data.keySet()) {
                if (name.startsWith(ATTR_PREFIX)) {
                    String attrName = name.substring(ATTR_PREFIX.length());
                    attributeMap.put(attrName, data.get(name));
                }
            }
        }

        @Override
        public boolean isModified() {
            return !modifications.isEmpty();
        }

        @Override
        public <T> Optional<T> get(CharSequence name, ArgumentConversionContext<T> conversionContext) {
            return super.get(name, ConversionContext.of(Object.class)).flatMap(o -> {
                if (o instanceof byte[] rawBytes) {
                    return valueSerializer.deserialize(rawBytes, conversionContext.getArgument());
                }
                return conversionService.convert(o, conversionContext);
            });
        }

        @Override
        public Optional<Object> get(CharSequence attr) {
            Optional<Object> result = super.get(attr);
            if (result.isPresent()) {
                Object val = result.get();
                if (val instanceof byte[]) {
                    Optional<Object> deserialized = valueSerializer.deserialize((byte[]) val);
                    deserialized.ifPresent(t -> attributeMap.put(attr, t));
                    return deserialized;
                }
            }
            return result;
        }

        @Override
        public Session setLastAccessedTime(Instant instant) {
            if (instant != null) {
                if (!isNew()) {
                    this.modifications.add(Modification.ADDITION);
                }
                if (writeMode == RedisHttpSessionConfiguration.WriteMode.BACKGROUND) {
                    byte[] lastAccessedTimeBytes = String.valueOf(instant.toEpochMilli()).getBytes();
                    writeBehind(ATTR_LAST_ACCESSED, lastAccessedTimeBytes);
                }
            }
            return super.setLastAccessedTime(instant);
        }

        @Override
        public Session setMaxInactiveInterval(Duration duration) {
            if (duration != null) {

                if (!isNew()) {
                    this.modifications.add(Modification.ADDITION);
                }
                if (writeMode == RedisHttpSessionConfiguration.WriteMode.BACKGROUND) {
                    byte[] intervalBytes = String.valueOf(getMaxInactiveInterval().getSeconds()).getBytes();
                    writeBehind(ATTR_MAX_INACTIVE_INTERVAL, intervalBytes);
                }
            }
            return super.setMaxInactiveInterval(duration);
        }

        @Override
        public MutableConvertibleValues<Object> put(CharSequence key, Object value) {
            if (value == null) {
                return remove(key);
            } else {
                if (key != null && !isNew()) {
                    this.modifications.add(Modification.ADDITION);
                    String attr = key.toString();
                    this.modifiedKeys.add(attr);
                    if (writeMode == RedisHttpSessionConfiguration.WriteMode.BACKGROUND) {
                        byte[] bytes = value instanceof byte[] ? (byte[]) value : valueSerializer.serialize(value).orElse(null);
                        if (bytes != null) {
                            writeBehind(ATTR_PREFIX + attr, bytes);
                        }
                    }
                }
                return super.put(key, value);
            }
        }

        @Override
        public MutableConvertibleValues<Object> remove(CharSequence key) {
            if (key != null && !isNew()) {
                this.modifications.add(Modification.REMOVAL);
                String attr = key.toString();
                this.removedKeys.add(attr);
                if (writeMode == RedisHttpSessionConfiguration.WriteMode.BACKGROUND) {
                    redisHashAsyncCommands.hdel(getSessionKey(getId()), getAttributeKey(attr))
                            .exceptionally(attributeErrorHandler(attr));
                }
            }
            this.modifications.add(Modification.REMOVAL);
            return super.remove(key);
        }

        private byte[] getAttributeKey(String attr) {
            return (ATTR_PREFIX + attr).getBytes(charset);
        }

        @Override
        public MutableConvertibleValues<Object> clear() {
            if (!isNew()) {

                this.modifications.add(Modification.CLEARED);
                Set<String> names = names();
                this.removedKeys.addAll(names);
                if (writeMode == RedisHttpSessionConfiguration.WriteMode.BACKGROUND) {
                    byte[][] attributes = names.stream().map(this::getAttributeKey).toArray(byte[][]::new);
                    redisHashAsyncCommands.hdel(getSessionKey(getId()), attributes)
                            .exceptionally(throwable -> {
                                if (LOG.isErrorEnabled()) {
                                    LOG.error("Error writing behind session attributes: " + throwable.getMessage(), throwable);
                                }
                                return null;
                            });
                }
            }
            return super.clear();
        }

        @Override
        public boolean isNew() {
            return modifications.contains(Modification.CREATED);
        }

        /**
         * @param charset The charset to evaluate
         * @return Produces a modification delta with the changes necessary to save the session
         */
        Map<byte[], byte[]> delta(Charset charset) {
            if (modifications.isEmpty()) {
                return Collections.emptyMap();
            } else {
                Map<byte[], byte[]> delta = new LinkedHashMap<>();
                if (isNew()) {
                    byte[] creationTimeBytes = String.valueOf(getCreationTime().toEpochMilli()).getBytes();
                    delta.put(ATTR_CREATION_TIME.getBytes(charset), creationTimeBytes);
                    Instant lastAccessedTime = getLastAccessedTime();
                    byte[] lastAccessedTimeBytes = String.valueOf(lastAccessedTime.toEpochMilli()).getBytes();

                    delta.put(ATTR_LAST_ACCESSED.getBytes(charset), lastAccessedTimeBytes);
                    delta.put(ATTR_MAX_INACTIVE_INTERVAL.getBytes(charset), String.valueOf(getMaxInactiveInterval().getSeconds()).getBytes());
                    for (CharSequence key : attributeMap.keySet()) {
                        convertAttribute(key, delta, charset);
                    }
                } else {
                    delta.put(ATTR_LAST_ACCESSED.getBytes(charset), String.valueOf(getLastAccessedTime().toEpochMilli()).getBytes());
                    delta.put(ATTR_MAX_INACTIVE_INTERVAL.getBytes(charset), String.valueOf(getMaxInactiveInterval().getSeconds()).getBytes());
                    for (CharSequence modifiedKey : modifiedKeys) {
                        convertAttribute(modifiedKey, delta, charset);
                    }
                }

                return delta;
            }
        }

        /**
         * Clear member attributes.
         */
        void clearModifications() {
            modifications.clear();
            removedKeys.clear();
            modifiedKeys.clear();
        }

        private <T> Function<Throwable, T> attributeErrorHandler(String attr) {
            return throwable -> {
                if (LOG.isErrorEnabled()) {
                    LOG.error("Error writing behind session attribute [" + attr + "]: " + throwable.getMessage(), throwable);
                }
                return null;
            };
        }

        private void writeBehind(String attr, byte[] lastAccessedTimeBytes) {
            redisHashAsyncCommands.hset(getSessionKey(getId()), attr.getBytes(charset), lastAccessedTimeBytes)
                    .exceptionally(attributeErrorHandler(attr));
        }

        private void convertAttribute(CharSequence key, Map<byte[], byte[]> delta, Charset charset) {
            Object rawValue = attributeMap.get(key);
            byte[] attributeKey = getAttributeKey(key.toString());
            if (rawValue instanceof byte[]) {
                delta.put(attributeKey, (byte[]) rawValue);
            } else if (rawValue != null) {
                Optional<byte[]> serialized = valueSerializer.serialize(rawValue);
                serialized.ifPresent(bytes -> delta.put(attributeKey, bytes));
            }
        }

    }
}
