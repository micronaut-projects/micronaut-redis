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
package io.micronaut.configuration.lettuce.pubsub.intercept;

import io.micronaut.aop.InterceptedMethod;
import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.configuration.lettuce.pubsub.RedisMessageBodyHandler;
import io.micronaut.configuration.lettuce.pubsub.RedisPubSubPublisher;
import io.micronaut.configuration.lettuce.pubsub.annotation.MessageChannel;
import io.micronaut.configuration.lettuce.pubsub.annotation.RedisPubSubClient;
import io.micronaut.context.BeanContext;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.http.MediaType;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.exceptions.MessagingClientException;
import io.micronaut.scheduling.TaskExecutors;
import jakarta.inject.Singleton;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Introduction advice for {@link RedisPubSubClient}.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
@InterceptorBean(RedisPubSubClient.class)
@Internal
class RedisPubSubClientIntroductionAdvice implements MethodInterceptor<Object, Object> {

    private final BeanContext beanContext;
    private final ConversionService conversionService;
    private final RedisPubSubPublisher publisher;
    private final RedisMessageBodyHandler messageBodyHandler;

    RedisPubSubClientIntroductionAdvice(BeanContext beanContext,
                                        ConversionService conversionService,
                                        RedisPubSubPublisher publisher,
                                        RedisMessageBodyHandler messageBodyHandler) {
        this.beanContext = beanContext;
        this.conversionService = conversionService;
        this.publisher = publisher;
        this.messageBodyHandler = messageBodyHandler;
    }

    @Override
    public Object intercept(MethodInvocationContext<Object, Object> context) {
        if (!context.hasAnnotation(RedisPubSubClient.class)) {
            return context.proceed();
        }
        InterceptedMethod interceptedMethod = InterceptedMethod.of(context, conversionService);
        try {
            PublishInvocation invocation = resolveInvocation(context);
            return switch (interceptedMethod.resultType()) {
                case SYNCHRONOUS -> convertResult(context.getReturnType().asArgument(), publish(invocation), invocation.bodyValue());
                case COMPLETION_STAGE -> interceptedMethod.handleResult(publishAsync(context, invocation, interceptedMethod.returnTypeValue()));
                case PUBLISHER -> interceptedMethod.handleResult(Publishers.fromCompletableFuture(() -> publishAsync(context, invocation, interceptedMethod.returnTypeValue())));
                default -> interceptedMethod.unsupported();
            };
        } catch (Exception e) {
            return interceptedMethod.handleException(e);
        }
    }

    private CompletableFuture<Object> publishAsync(MethodInvocationContext<Object, Object> context,
                                                   PublishInvocation invocation,
                                                   Argument<?> returnType) {
        String executorName = context.stringValue(RedisPubSubClient.class, "executor").orElse(TaskExecutors.BLOCKING);
        ExecutorService executor = beanContext.findBean(ExecutorService.class, Qualifiers.byName(executorName))
            .orElseThrow(() -> new ConfigurationException("No executor named [" + executorName + "] configured for Redis Pub/Sub client: " + context));
        return CompletableFuture.supplyAsync(() -> convertResult(returnType, publish(invocation), invocation.bodyValue()), executor);
    }

    private long publish(PublishInvocation invocation) {
        return publisher.publishOnConnection(
            invocation.connectionName(),
            invocation.channel(),
            invocation.bodyArgument(),
            invocation.mediaType(),
            invocation.bodyValue()
        );
    }

    private Object convertResult(Argument<?> returnType, long result, Object bodyValue) {
        Class<?> javaReturnType = returnType.getType();
        if (javaReturnType == void.class || javaReturnType == Void.class) {
            return null;
        }
        Optional<?> converted = conversionService.convert(result, returnType);
        if (converted.isPresent()) {
            return converted.get();
        }
        if (bodyValue != null && javaReturnType.isInstance(bodyValue)) {
            return bodyValue;
        }
        return null;
    }

    private PublishInvocation resolveInvocation(MethodInvocationContext<Object, Object> context) {
        String channel = resolveChannel(context);
        BodyParameter bodyParameter = resolveBody(context);
        String connectionName = context.stringValue(RedisPubSubClient.class)
            .filter(s -> !s.isEmpty())
            .orElse(null);
        BeanDefinition<Object> beanDefinition = beanContext.getBeanDefinition((Class<Object>) context.getDeclaringType());
        MediaType mediaType = messageBodyHandler.resolveOutgoingMediaType(context.getAnnotationMetadata(), beanDefinition.getAnnotationMetadata());
        return new PublishInvocation(connectionName, channel, bodyParameter.argument(), mediaType, bodyParameter.value());
    }

    private String resolveChannel(MethodInvocationContext<Object, Object> context) {
        Argument<?>[] arguments = context.getArguments();
        Object[] parameterValues = context.getParameterValues();
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].isAnnotationPresent(MessageChannel.class)) {
                return conversionService.convert(parameterValues[i], String.class)
                    .filter(s -> !s.isEmpty())
                    .orElseThrow(() -> new MessagingClientException("Cannot resolve @MessageChannel parameter for Redis Pub/Sub client method: " + context));
            }
        }
        AnnotationMetadata annotationMetadata = context.getAnnotationMetadata();
        String methodChannel = annotationMetadata.stringValue(MessageChannel.class).orElse("");
        if (!methodChannel.isEmpty()) {
            return methodChannel;
        }
        String[] channels = annotationMetadata.stringValues(MessageChannel.class, "channels");
        if (channels.length > 0 && !channels[0].isEmpty()) {
            return channels[0];
        }
        throw new MessagingClientException("A Redis Pub/Sub client method must declare a @MessageChannel value or parameter: " + context);
    }

    private BodyParameter resolveBody(MethodInvocationContext<Object, Object> context) {
        Argument<?>[] arguments = context.getArguments();
        Object[] parameterValues = context.getParameterValues();
        BodyParameter resolved = null;
        for (int i = 0; i < arguments.length; i++) {
            Argument<?> argument = arguments[i];
            if (argument.isAnnotationPresent(MessageChannel.class)) {
                continue;
            }
            if (argument.isAnnotationPresent(MessageBody.class)) {
                return new BodyParameter(argument, parameterValues[i]);
            }
            if (resolved == null) {
                resolved = new BodyParameter(argument, parameterValues[i]);
            } else {
                throw new MessagingClientException("Multiple possible message body arguments found for Redis Pub/Sub client method: " + context);
            }
        }
        return resolved == null ? new BodyParameter(Argument.OBJECT_ARGUMENT, null) : resolved;
    }

    private record PublishInvocation(String connectionName, String channel, Argument<?> bodyArgument, MediaType mediaType, Object bodyValue) {
    }

    private record BodyParameter(Argument<?> argument, Object value) {
        private BodyParameter {
            Objects.requireNonNull(argument, "argument");
        }
    }
}
