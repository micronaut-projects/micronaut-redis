package io.micronaut.configuration.lettuce.pubsub.bind;

import io.micronaut.configuration.lettuce.pubsub.RedisMessage;
import io.micronaut.core.bind.ArgumentBinder;

/**
 * Interface for binders that bind method arguments from a {@link RedisMessage}.
 *
 * @param <T> The target type
 * @author Cristian Morales
 */
@SuppressWarnings("WeakerAccess")
public interface RedisArgumentBinder<T> extends ArgumentBinder<T, RedisMessage> {
}
