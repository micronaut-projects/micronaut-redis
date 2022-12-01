
package io.micronaut.configuration.lettuce.pubsub;

import io.micronaut.core.async.subscriber.TypedSubscriber;
import io.micronaut.core.bind.BoundExecutable;
import io.micronaut.core.type.Argument;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

@SuppressWarnings({"rawtypes", "unchecked"})
public class RedisSubscriber extends TypedSubscriber<RedisMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSubscriber.class);

    private final Function<RedisMessage, BoundExecutable> onNext;
    private final Object bean;

    public RedisSubscriber(Object bean, Function<RedisMessage, BoundExecutable> onNext) {
        super(Argument.of(RedisMessage.class));
        this.onNext = onNext;
        this.bean = bean;
    }

    @Override
    protected void doOnSubscribe(Subscription subscription) {
        this.subscription = subscription;
        this.subscription.request(1);
    }

    @Override
    protected void doOnNext(RedisMessage message) {
        BoundExecutable executable = onNext.apply(message);
        executable.invoke(bean);
    }

    @Override
    protected void doOnError(Throwable t) {
        LOG.error("Error", t);
    }

    @Override
    protected void doOnComplete() {
        LOG.info("Closing");
    }

}
