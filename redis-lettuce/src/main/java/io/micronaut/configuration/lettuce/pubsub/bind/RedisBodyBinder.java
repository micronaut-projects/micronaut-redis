package io.micronaut.configuration.lettuce.pubsub.bind;

import io.micronaut.configuration.lettuce.pubsub.RedisMessage;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.messaging.annotation.MessageBody;
import jakarta.inject.Singleton;

import java.util.Optional;

@Singleton
public class RedisBodyBinder<T> implements AnnotatedRedisMessageBinder<MessageBody, T>{

    @Override
    public Class<MessageBody> annotationType() {
        return MessageBody.class;
    }

    @Override
    public BindingResult<T> bind(ArgumentConversionContext<T> context, RedisMessage source) {
        Object value = source.getBody();
        Optional<T> converted = ConversionService.SHARED.convert(value, context);
        return () -> converted;
    }

}
