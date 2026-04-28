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
package io.micronaut.configuration.lettuce.pubsub;

import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.io.buffer.ByteArrayBufferFactory;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Consumes;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.body.MessageBodyHandlerRegistry;
import io.micronaut.http.body.MessageBodyReader;
import io.micronaut.http.body.MessageBodyWriter;
import io.micronaut.http.codec.CodecException;
import io.micronaut.http.simple.SimpleHttpHeaders;
import jakarta.inject.Singleton;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Optional;

/**
 * Handles payload serialization for Redis Pub/Sub messages.
 *
 * @author Graeme Rocher
 * @since 7.0
 */
@Singleton
@Internal
public final class RedisMessageBodyHandler {

    private final MessageBodyHandlerRegistry messageBodyHandlerRegistry;
    private final ConversionService conversionService;
    private final RedisPubSubConfiguration configuration;

    public RedisMessageBodyHandler(MessageBodyHandlerRegistry messageBodyHandlerRegistry,
                                   ConversionService conversionService,
                                   RedisPubSubConfiguration configuration) {
        this.messageBodyHandlerRegistry = messageBodyHandlerRegistry;
        this.conversionService = conversionService;
        this.configuration = configuration;
    }

    public byte[] serialize(Object value) {
        return serialize(
            (Argument<?>) Argument.of(value == null ? Object.class : value.getClass()),
            AnnotationMetadata.EMPTY_METADATA,
            AnnotationMetadata.EMPTY_METADATA,
            value
        );
    }

    /**
     * Serialize a Redis Pub/Sub body.
     *
     * @param argument  The body argument
     * @param mediaType The media type
     * @param value     The value
     * @return The serialized bytes
     */
    public byte[] serialize(Argument<?> argument, MediaType mediaType, Object value) {
        if (value == null) {
            return new byte[0];
        }
        return writeBody(argument, mediaType, value);
    }

    /**
     * Serialize a Redis Pub/Sub body using only method-level annotation metadata.
     * Equivalent to calling {@link #serialize(Argument, AnnotationMetadata, AnnotationMetadata, Object)}
     * with {@link AnnotationMetadata#EMPTY_METADATA} as the declaring type metadata.
     *
     * @param argument           The body argument
     * @param annotationMetadata The method or element annotation metadata
     * @param value              The value
     * @return The serialized bytes
     */
    public byte[] serialize(Argument<?> argument, AnnotationMetadata annotationMetadata, Object value) {
        return serialize(argument, annotationMetadata, AnnotationMetadata.EMPTY_METADATA, value);
    }

    /**
     * Serialize a Redis Pub/Sub body.
     *
     * @param argument              The body argument
     * @param annotationMetadata    The method or element annotation metadata
     * @param declaringTypeMetadata The declaring type annotation metadata
     * @param value                 The value
     * @return The serialized bytes
     */
    public byte[] serialize(Argument<?> argument,
                            AnnotationMetadata annotationMetadata,
                            AnnotationMetadata declaringTypeMetadata,
                            Object value) {
        if (value == null) {
            return new byte[0];
        }
        MediaType mediaType = resolveOutgoingMediaType(annotationMetadata, declaringTypeMetadata);
        return writeBody(argument, mediaType, value);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private byte[] writeBody(Argument<?> argument, MediaType mediaType, Object value) {
        MessageBodyWriter writer = messageBodyHandlerRegistry.getWriter((Argument) argument, List.of(mediaType));
        SimpleHttpHeaders headers = new SimpleHttpHeaders(conversionService);
        headers.add(HttpHeaders.CONTENT_TYPE, mediaType.toString());
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            writer.writeTo(argument, mediaType, value, headers, outputStream);
            return outputStream.toByteArray();
        } catch (CodecException e) {
            throw new IllegalArgumentException("Cannot serialize Redis Pub/Sub body of type [" + argument.getType().getName() + "]", e);
        }
    }

    /**
     * Deserialize a Redis Pub/Sub body.
     *
     * @param value     The serialized bytes
     * @param context   The conversion context
     * @param mediaType The media type
     * @param <T>       The target type
     * @return The deserialized value
     */
    public <T> Optional<T> deserialize(byte[] value, ArgumentConversionContext<T> context, MediaType mediaType) {
        MessageBodyReader<T> reader = messageBodyHandlerRegistry.getReader(context.getArgument(), List.of(mediaType));
        SimpleHttpHeaders headers = new SimpleHttpHeaders(conversionService);
        headers.add(HttpHeaders.CONTENT_TYPE, mediaType.toString());
        try {
            return Optional.ofNullable(reader.read(context.getArgument(), mediaType, headers, ByteArrayBufferFactory.INSTANCE.wrap(value)));
        } catch (CodecException e) {
            throw new IllegalArgumentException("Cannot deserialize Redis Pub/Sub body to type [" + context.getArgument().getType().getName() + "]", e);
        }
    }

    /**
     * Resolve the media type to use for serializing a client method body.
     *
     * @param annotationMetadata    The method annotation metadata
     * @param declaringTypeMetadata The declaring type annotation metadata
     * @return The media type
     */
    public MediaType resolveOutgoingMediaType(AnnotationMetadata annotationMetadata, AnnotationMetadata declaringTypeMetadata) {
        String[] values = annotationMetadata.stringValues(Produces.class);
        if (values.length > 0) {
            return new MediaType(values[0]);
        }
        values = declaringTypeMetadata.stringValues(Produces.class);
        if (values.length > 0) {
            return new MediaType(values[0]);
        }
        return configuration.getDefaultBodyMediaType();
    }

    /**
     * Resolve the media type to use for deserializing a listener method body.
     *
     * @param annotationMetadata         The method annotation metadata
     * @param declaringTypeMetadata      The declaring type annotation metadata
     * @return The media type
     */
    public MediaType resolveIncomingMediaType(AnnotationMetadata annotationMetadata, AnnotationMetadata declaringTypeMetadata) {
        String[] values = annotationMetadata.stringValues(Consumes.class);
        if (values.length > 0) {
            return new MediaType(values[0]);
        }
        values = declaringTypeMetadata.stringValues(Consumes.class);
        if (values.length > 0) {
            return new MediaType(values[0]);
        }
        return configuration.getDefaultBodyMediaType();
    }

}
