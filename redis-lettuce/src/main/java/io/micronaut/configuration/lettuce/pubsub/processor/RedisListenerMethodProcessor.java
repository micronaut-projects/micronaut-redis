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
package io.micronaut.configuration.lettuce.pubsub.processor;

import io.micronaut.configuration.lettuce.AbstractRedisConfiguration;
import io.micronaut.configuration.lettuce.pubsub.RedisListenerMessage;
import io.micronaut.configuration.lettuce.pubsub.RedisMessageBodyHandler;
import io.micronaut.configuration.lettuce.pubsub.RedisPubSubListenerRegistry;
import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel;
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisListener;
import io.micronaut.configuration.lettuce.pubsub.bind.RedisBinderRegistry;
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerException;
import io.micronaut.configuration.lettuce.pubsub.exception.RedisListenerExceptionHandler;
import io.micronaut.context.BeanContext;
import io.micronaut.context.Qualifier;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.bind.BoundExecutable;
import io.micronaut.core.bind.DefaultExecutableBinder;
import io.micronaut.core.util.StringUtils;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.http.MediaType;
import io.micronaut.scheduling.TaskExecutors;
import jakarta.inject.Singleton;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Registers Redis Pub/Sub listener methods.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
@Requires(beans = AbstractRedisConfiguration.class)
public class RedisListenerMethodProcessor implements ExecutableMethodProcessor<MessageChannel> {

    private final BeanContext beanContext;
    private final RedisBinderRegistry binderRegistry;
    private final RedisPubSubListenerRegistry listenerRegistry;
    private final RedisMessageBodyHandler messageBodyHandler;

    /**
     * @param beanContext      The bean context
     * @param binderRegistry   The binder registry
     * @param listenerRegistry The listener registry
     * @param messageBodyHandler The message body handler
     */
    public RedisListenerMethodProcessor(BeanContext beanContext,
                                        RedisBinderRegistry binderRegistry,
                                        RedisPubSubListenerRegistry listenerRegistry,
                                        RedisMessageBodyHandler messageBodyHandler) {
        this.beanContext = beanContext;
        this.binderRegistry = binderRegistry;
        this.listenerRegistry = listenerRegistry;
        this.messageBodyHandler = messageBodyHandler;
    }

    @Override
    public <B> void process(BeanDefinition<B> beanDefinition, ExecutableMethod<B, ?> method) {
        AnnotationValue<RedisListener> listener = beanDefinition.getAnnotation(RedisListener.class);
        if (listener == null) {
            return;
        }
        AnnotationValue<MessageChannel> messageChannel = method.getAnnotation(MessageChannel.class);
        if (messageChannel == null) {
            return;
        }

        Object bean = getExecutableMethodBean(beanDefinition, method);
        Set<RedisPubSubListenerRegistry.ChannelSubscription> subscriptions = resolveSubscriptions(messageChannel, method);
        ExecutorService executor = resolveExecutor(listener, method);
        String connectionName = listener.stringValue("connection").filter(StringUtils::isNotEmpty).orElse(null);
        DefaultExecutableBinder<RedisListenerMessage> executableBinder = new DefaultExecutableBinder<>();
        MediaType mediaType = messageBodyHandler.resolveIncomingMediaType(method.getAnnotationMetadata(), beanDefinition.getAnnotationMetadata());
        RedisListenerExceptionHandler exceptionHandler = resolveExceptionHandler(messageChannel, method);

        listenerRegistry.subscribe(connectionName, subscriptions, executor, message -> {
            try {
                BoundExecutable<B, ?> boundExecutable = executableBinder.bind(
                    method,
                    binderRegistry,
                    new RedisListenerMessage(message, mediaType)
                );
                boundExecutable.invoke((B) bean);
            } catch (Throwable e) {
                exceptionHandler.handle(new RedisListenerException(
                    "Error invoking Redis Pub/Sub listener method [" + method + "]",
                    e instanceof Exception exception ? exception : new RuntimeException(e),
                    bean,
                    message,
                    resolveChannelName(messageChannel)
                ));
            }
        });
    }

    private ExecutorService resolveExecutor(AnnotationValue<RedisListener> listener, ExecutableMethod<?, ?> method) {
        String executorName = listener.stringValue("executor").orElse(TaskExecutors.BLOCKING);
        return beanContext.findBean(ExecutorService.class, Qualifiers.byName(executorName))
            .orElseThrow(() -> new ConfigurationException(
                "Could not find executor service [" + executorName + "] specified for Redis listener method [" + method + "]"
            ));
    }

    private Set<RedisPubSubListenerRegistry.ChannelSubscription> resolveSubscriptions(AnnotationValue<MessageChannel> annotationValue,
                                                                                      ExecutableMethod<?, ?> method) {
        Set<RedisPubSubListenerRegistry.ChannelSubscription> subscriptions = new LinkedHashSet<>();
        annotationValue.stringValue()
            .filter(StringUtils::isNotEmpty)
            .ifPresent(value -> subscriptions.add(new RedisPubSubListenerRegistry.ChannelSubscription(value, false)));
        Arrays.stream(annotationValue.stringValues("channels"))
            .filter(StringUtils::isNotEmpty)
            .forEach(value -> subscriptions.add(new RedisPubSubListenerRegistry.ChannelSubscription(value, false)));
        Arrays.stream(annotationValue.stringValues("patterns"))
            .filter(StringUtils::isNotEmpty)
            .forEach(value -> subscriptions.add(new RedisPubSubListenerRegistry.ChannelSubscription(value, true)));
        if (subscriptions.isEmpty()) {
            throw new ConfigurationException("Redis listener method [" + method + "] must declare at least one channel or pattern");
        }
        return subscriptions;
    }

    private RedisListenerExceptionHandler resolveExceptionHandler(AnnotationValue<MessageChannel> annotationValue,
                                                                  ExecutableMethod<?, ?> method) {
        Class<? extends RedisListenerExceptionHandler> exceptionHandlerType = annotationValue.classValue("exceptionHandler", RedisListenerExceptionHandler.class)
            .orElse(RedisListenerExceptionHandler.class);
        if (exceptionHandlerType == RedisListenerExceptionHandler.class) {
            return beanContext.getBean(RedisListenerExceptionHandler.class);
        }
        return beanContext.findBean(exceptionHandlerType)
            .orElseThrow(() -> new ConfigurationException(
                "Could not find Redis listener exception handler [" + exceptionHandlerType.getName() + "] specified for method [" + method + "]"
            ));
    }

    private String resolveChannelName(AnnotationValue<MessageChannel> annotationValue) {
        return annotationValue.stringValue().filter(StringUtils::isNotEmpty)
            .orElseGet(() -> Arrays.stream(annotationValue.stringValues("channels"))
                .filter(StringUtils::isNotEmpty)
                .findFirst()
                .orElseGet(() -> Arrays.stream(annotationValue.stringValues("patterns"))
                    .filter(StringUtils::isNotEmpty)
                    .findFirst()
                    .orElse("<unknown>")));
    }

    @SuppressWarnings("unchecked")
    private <B> B getExecutableMethodBean(BeanDefinition<B> beanDefinition, ExecutableMethod<B, ?> method) {
        Qualifier<B> qualifier = beanDefinition
            .getAnnotationNameByStereotype(jakarta.inject.Qualifier.class)
            .map(type -> (Qualifier<B>) Qualifiers.byAnnotation(beanDefinition, type))
            .orElse(null);
        return beanContext.findBean(beanDefinition.getBeanType(), qualifier)
            .orElseThrow(() -> new ConfigurationException("Could not find bean for Redis listener method [" + method + "]"));
    }
}
