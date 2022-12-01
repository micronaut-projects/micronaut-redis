package io.micronaut.configuration.lettuce.pubsub.bind;

import io.micronaut.configuration.lettuce.pubsub.RedisMessage;

import java.lang.annotation.Annotation;

/**
 * Interface for binders that bind method arguments from a {@link RedisMessage} via annotation.
 *
 * @param <T> The target type
 * @param <A> The annotation type
 * @author Cristian Morales
 * @since TODO
 */
public interface AnnotatedRedisMessageBinder<A extends Annotation, T> extends RedisArgumentBinder<T> {

    /**
     * @return The annotation type
     */
    Class<A> annotationType();
}
