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

import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanRegistration;
import io.micronaut.context.Qualifier;
import io.micronaut.context.WatchableBeanContext;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.env.DevelopmentActive;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.context.reload.ClassChange;
import io.micronaut.context.reload.ClassChangeEvent;
import io.micronaut.context.reload.ReloadStrategy;
import io.micronaut.context.watch.BeanDefinitionChange;
import io.micronaut.context.watch.BeanDefinitionWatcher;
import io.micronaut.context.watch.ClassChangeWatcher;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.order.Ordered;
import io.micronaut.core.reflect.ClassUtils;
import io.micronaut.core.serialize.ObjectSerializer;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.BeanDefinitionReference;
import io.micronaut.inject.BeanType;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Keeps the Redis modules in step with the code in development mode. It exists only in development mode, so nothing of
 * it is on the path of a command, a cached invocation or a message. It serves every Redis module: the cache and Pub/Sub
 * types are resolved by name, and followed only when their module is present.
 *
 * <p><b>Across a restart.</b> Development mode keeps the clients, with their resources, and their connections across a
 * restart of the application, until a change under {@code redis} releases them. As the next generation adopts the
 * {@link RedisModuleConnections} it kept, this bean drops the connections that were closed with a client that was not
 * kept, so that nothing of that client stays reachable. The Pub/Sub connection beans are not kept: the application adds
 * listeners to them, which Lettuce offers no way to list, so they are closed with the generation that added them,
 * together with its listeners and subscriptions.</p>
 *
 * <p><b>In place.</b></p>
 * <ul>
 *     <li>A Redis cache resolves its key and value serializers and its expiration policy as it is created, and keeps
 *     them. A definition of an {@link ObjectSerializer} or an expiration policy registered or removed, or a class of one
 *     of them changed in place, recreates the Redis caches: the beans that received one, such as the cache manager, are
 *     destroyed with it, as the dependency graph records, and created again on top of the new caches, which receive
 *     the same connection from {@link RedisModuleConnections}.</li>
 *     <li>A definition of a {@code @RedisListener} bean or of a Pub/Sub listener exception handler registered or
 *     removed, or a class of one of them changed in place, re-registers the listeners: the changed beans are recreated,
 *     then the listener method processor, which removes the listener methods it registered as it is destroyed. The
 *     context creates it again at once and gives it the listener methods, so that it registers them again, on top of
 *     the new beans, on the same connection: a channel no listener uses any more is unsubscribed.</li>
 *     <li>A class change applied in place that retires a classloader does both.</li>
 *     <li>A class change applied in place that touches none of these recreates nothing.</li>
 * </ul>
 *
 * <p>Each bean is recreated through {@link WatchableBeanContext#recreate(Object)}; a context that does not track bean
 * dependencies recreates nothing, and the caches and listeners stay as they are until a restart. The watches run after
 * those of other modules, so that a bean another module recreates for the same change, such as a JSON mapper the
 * message body handler received, is in place first. It holds the context only, never a Redis bean: a bean that received
 * one is a dependent of it, which recreating it would destroy along with its watches.</p>
 *
 * @author graemerocher
 * @since 7.3.0
 */
@Internal
@Context
@DevelopmentActive
final class DevelopmentRedisReloader implements BeanCreatedEventListener<RedisModuleConnections> {

    private static final Logger LOG = LoggerFactory.getLogger(DevelopmentRedisReloader.class);

    private static final String ABSTRACT_REDIS_CACHE = "io.micronaut.configuration.lettuce.cache.AbstractRedisCache";
    private static final String EXPIRATION_POLICY = "io.micronaut.configuration.lettuce.cache.expiration.ExpirationAfterWritePolicy";
    private static final String REDIS_LISTENER = "io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener";
    private static final String EXCEPTION_HANDLER = "io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerExceptionHandler";
    private static final String METHOD_PROCESSOR = "io.micronaut.configuration.lettuce.pubsub.processor.RedisListenerMethodProcessor";

    private final BeanContext beanContext;
    /**
     * The caches, when the cache module is present.
     */
    private final @Nullable Class<?> cacheType;
    /**
     * The types a cache resolves as it is created, when the cache module is present.
     */
    private final List<Class<?>> cacheResolvedTypes;
    /**
     * The listener method processor, when the Pub/Sub module is present.
     */
    private final @Nullable Class<?> processorType;
    private final @Nullable Class<?> exceptionHandlerType;
    private final Qualifier<Object> listeners = new ListenerQualifier();

    /**
     * @param beanContext The context, watched when it can be
     */
    DevelopmentRedisReloader(BeanContext beanContext) {
        this.beanContext = beanContext;
        ClassLoader loader = DevelopmentRedisReloader.class.getClassLoader();
        this.cacheType = ClassUtils.forName(ABSTRACT_REDIS_CACHE, loader).orElse(null);
        Class<?> expirationPolicy = ClassUtils.forName(EXPIRATION_POLICY, loader).orElse(null);
        this.cacheResolvedTypes = cacheType == null || expirationPolicy == null ? List.of() : List.of(ObjectSerializer.class, expirationPolicy);
        this.processorType = ClassUtils.forName(METHOD_PROCESSOR, loader).orElse(null);
        this.exceptionHandlerType = ClassUtils.forName(EXCEPTION_HANDLER, loader).orElse(null);
        if (beanContext instanceof WatchableBeanContext watchable) {
            // by type and by stereotype, so that no other definition is loaded
            for (Class<?> resolved : cacheResolvedTypes) {
                watchable.definitions(resolved).watch(new DefinitionsWatcher<>(false));
            }
            if (processorType != null) {
                watchable.definitions().qualifier(Qualifiers.byStereotype(REDIS_LISTENER)).watch(new DefinitionsWatcher<>(true));
                if (exceptionHandlerType != null) {
                    watchable.definitions(exceptionHandlerType).watch(new DefinitionsWatcher<>(true));
                }
            }
            if (cacheType != null || processorType != null) {
                watchable.classChanges().watch(new ClassWatcher());
            }
        }
    }

    /**
     * Drops the connections a client that was not kept closed, on the module connections kept from the previous
     * generation, as the context runs its listeners on what it adopts. On a module connections bean created afresh
     * there is nothing to drop.
     *
     * @param event The event
     * @return The bean
     */
    @Override
    public RedisModuleConnections onCreated(BeanCreatedEvent<RedisModuleConnections> event) {
        RedisModuleConnections connections = event.getBean();
        connections.releaseClosed();
        return connections;
    }

    private void onClassChange(ClassChangeEvent change) {
        if (change.strategy() == ReloadStrategy.RESTART) {
            // the next generation creates its own caches and subscribes again, on the connections that are kept
            return;
        }
        if (!change.retiredLoaders().isEmpty()) {
            recreateCaches("a reload retired a classloader");
            reregisterListeners(List.of(), "a reload retired a classloader");
            return;
        }
        boolean caches = false;
        List<String> changedListeners = new ArrayList<>();
        for (ClassChange classChange : change.changes()) {
            String className = classChange.className();
            Class<?> type = load(className, change.newLoader());
            if (!cacheResolvedTypes.isEmpty() && (isAssignable(cacheResolvedTypes, type) || wasDefinedAs(className, false))) {
                caches = true;
            }
            if (processorType != null && (isListener(type) || wasDefinedAs(className, true))) {
                changedListeners.add(className);
            }
        }
        if (caches) {
            recreateCaches("a serializer or an expiration policy changed");
        }
        if (!changedListeners.isEmpty()) {
            reregisterListeners(changedListeners, changedListeners + " changed");
        }
    }

    private static @Nullable Class<?> load(String className, ClassLoader loader) {
        try {
            return Class.forName(className, false, loader);
        } catch (ClassNotFoundException | LinkageError e) {
            // removed, or not loadable on its own: nothing of the new generation is built from it
            return null;
        }
    }

    private static boolean isAssignable(List<Class<?>> types, @Nullable Class<?> type) {
        if (type == null) {
            return false;
        }
        for (Class<?> candidate : types) {
            if (candidate.isAssignableFrom(type)) {
                return true;
            }
        }
        return false;
    }

    private boolean isListener(@Nullable Class<?> type) {
        if (type == null) {
            return false;
        }
        try {
            for (Annotation annotation : type.getAnnotations()) {
                if (annotation.annotationType().getName().equals(REDIS_LISTENER)) {
                    return true;
                }
            }
            return exceptionHandlerType != null && exceptionHandlerType.isAssignableFrom(type);
        } catch (LinkageError e) {
            return false;
        }
    }

    private boolean isListenerBean(BeanType<?> candidate) {
        return candidate.getAnnotationMetadata().hasStereotype(REDIS_LISTENER)
            || exceptionHandlerType != null && exceptionHandlerType.isAssignableFrom(candidate.getBeanType());
    }

    /**
     * Whether the class a change replaces was a listener or exception handler bean, or a serializer or expiration policy
     * bean, as the context was compiled. The references are matched by name, so that only the definitions of that class
     * are loaded, and nothing of a previous class is kept.
     */
    private boolean wasDefinedAs(String className, boolean listener) {
        int lastDot = className.lastIndexOf('.');
        // $Name$Definition and the definitions of its proxies
        String definition = className.substring(0, lastDot + 1) + '$' + className.substring(lastDot + 1) + "$Definition";
        for (BeanDefinitionReference<?> reference : beanContext.getBeanDefinitionReferences()) {
            String name = reference.getBeanDefinitionName();
            if (!name.equals(definition) && !name.startsWith(definition + '$')) {
                continue;
            }
            try {
                if (listener ? isListenerBean(reference) : isAssignable(cacheResolvedTypes, reference.getBeanType())) {
                    return true;
                }
            } catch (RuntimeException | LinkageError e) {
                // a definition of that name that no longer loads: what it was is unknown, so it counts
                return true;
            }
        }
        return false;
    }

    private void recreateCaches(String reason) {
        if (cacheType == null || !(beanContext instanceof WatchableBeanContext context)) {
            return;
        }
        List<Object> caches = new ArrayList<>();
        for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(cacheType)) {
            add(caches, registration.bean());
        }
        if (caches.isEmpty()) {
            return;
        }
        LOG.debug("Recreating the Redis caches: {}", reason);
        for (Object cache : caches) {
            context.recreate(cache);
        }
    }

    /**
     * Recreates the beans of the changed classes, then the listener method processor, which removes what it registered
     * as it is destroyed, and registers the methods again as the context creates it again.
     *
     * @param changed The changed listener and exception handler classes, whose beans are recreated
     * @param reason Why, for the log
     */
    private void reregisterListeners(List<String> changed, String reason) {
        if (processorType == null || !(beanContext instanceof WatchableBeanContext context)) {
            return;
        }
        // taken first: recreating one destroys the beans that received it, as the graph records them. The processor
        // goes last, as it resolves the listener beans as it is created again
        List<Object> beans = new ArrayList<>();
        if (!changed.isEmpty()) {
            Set<String> names = new HashSet<>(changed);
            for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(listeners)) {
                if (names.contains(registration.getBeanDefinition().getBeanType().getName())) {
                    add(beans, registration.bean());
                }
            }
        }
        for (BeanRegistration<?> registration : beanContext.getActiveBeanRegistrations(processorType)) {
            add(beans, registration.bean());
        }
        if (beans.isEmpty()) {
            return;
        }
        LOG.debug("Re-registering the Redis Pub/Sub listeners: {}", reason);
        for (Object bean : beans) {
            // false for a bean destroyed with one recreated before it: the context created it again already
            context.recreate(bean);
        }
    }

    private static void add(List<Object> beans, Object bean) {
        for (Object taken : beans) {
            if (taken == bean) {
                return;
            }
        }
        beans.add(bean);
    }

    private static List<String> names(BeanDefinitionChange<?> change) {
        List<String> changed = new ArrayList<>();
        for (BeanDefinition<?> definition : change.removed()) {
            changed.add(definition.getBeanType().getName());
        }
        for (BeanDefinition<?> definition : change.added()) {
            changed.add(definition.getBeanType().getName());
        }
        return changed;
    }

    /**
     * Follows the definitions of the serializers and expiration policies, or of the listeners and exception handlers.
     * The first batch is what the context started with.
     *
     * @param <T> The watched type
     */
    private final class DefinitionsWatcher<T> implements BeanDefinitionWatcher<T>, Ordered {
        private final boolean listener;

        private DefinitionsWatcher(boolean listener) {
            this.listener = listener;
        }

        @Override
        public void onChange(BeanDefinitionChange<T> change) {
            if (change.initial() || change.added().isEmpty() && change.removed().isEmpty()) {
                return;
            }
            if (listener) {
                reregisterListeners(names(change), "listener definitions changed");
            } else {
                recreateCaches("serializer or expiration policy definitions changed");
            }
        }

        @Override
        public int getOrder() {
            return Ordered.LOWEST_PRECEDENCE;
        }
    }

    /**
     * Selects the listener beans and the exception handlers, from their metadata and type, which needs nothing loaded
     * but the bean type.
     */
    private final class ListenerQualifier implements Qualifier<Object> {
        @Override
        public <B extends BeanType<Object>> Stream<B> reduce(Class<Object> beanType, Stream<B> candidates) {
            return candidates.filter(DevelopmentRedisReloader.this::isListenerBean);
        }

        @Override
        public boolean doesQualify(Class<Object> beanType, BeanType<Object> candidate) {
            return isListenerBean(candidate);
        }

        @Override
        public String toString() {
            return "Redis listeners and exception handlers";
        }
    }

    /**
     * Follows a class change applied in place.
     */
    private final class ClassWatcher implements ClassChangeWatcher, Ordered {
        @Override
        public void onChange(ClassChangeEvent change) {
            onClassChange(change);
        }

        @Override
        public int getOrder() {
            return Ordered.LOWEST_PRECEDENCE;
        }
    }
}
