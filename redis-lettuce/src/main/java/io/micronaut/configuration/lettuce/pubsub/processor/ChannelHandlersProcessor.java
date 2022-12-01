package io.micronaut.configuration.lettuce.pubsub.processor;

import io.micronaut.configuration.lettuce.pubsub.RedisMessage;
import io.micronaut.configuration.lettuce.pubsub.RedisSubscriber;
import io.micronaut.configuration.lettuce.pubsub.annotations.MessageChannel;
import io.micronaut.configuration.lettuce.pubsub.annotations.RedisListener;
import io.micronaut.configuration.lettuce.pubsub.bind.RedisBinderRegistry;
import io.micronaut.context.BeanContext;
import io.micronaut.context.Qualifier;
import io.micronaut.context.processor.ExecutableMethodProcessor;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.bind.BoundExecutable;
import io.micronaut.core.bind.DefaultExecutableBinder;
import io.micronaut.core.bind.ExecutableBinder;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ExecutableMethod;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

@Singleton
public class ChannelHandlersProcessor implements ExecutableMethodProcessor<MessageChannel> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSubscriber.class);

    private final RedisPubSub redisPubSub;
    private final BeanContext beanContext;
    private final RedisBinderRegistry redisBinderRegistry;

    public ChannelHandlersProcessor(RedisPubSub redisPubSub, BeanContext beanContext,
                                    RedisBinderRegistry redisBinderRegistry) {
        this.redisPubSub = redisPubSub;
        this.beanContext = beanContext;
        this.redisBinderRegistry = redisBinderRegistry;
        initializeListeners();
    }

    @Override
    public void process(BeanDefinition<?> beanDefinition, ExecutableMethod<?, ?> method) {
        AnnotationValue<RedisListener> subscriberConfig = beanDefinition.getAnnotation(RedisListener.class);

        if (subscriberConfig == null) {
            // TODO: add proper logging
            LOG.warn("Missing RedisListener");
            return;
        }

        AnnotationValue<MessageChannel> messageChannelAnnotation = method.getAnnotation(MessageChannel.class);

        final ExecutableBinder<RedisMessage> binder = new DefaultExecutableBinder<>();

        MessageHandler messageHandler = (message) -> binder.bind(method, redisBinderRegistry, message);
        Object bean = getExecutableMethodBean(beanContext, beanDefinition, method);
        RedisSubscriber subscriber = new RedisSubscriber(bean, messageHandler::handle);
        redisPubSub.subscribe(getMessageChannels(messageChannelAnnotation), subscriber);
    }

    @FunctionalInterface
    private interface MessageHandler {
        BoundExecutable<?, ?> handle(RedisMessage redisMessage);
    }

    /**
     * Pre-initialize singletons before processing
     */
    private void initializeListeners() {
        this.beanContext.getBeanDefinitions(Qualifiers.byType(RedisListener.class))
                .stream().filter(BeanDefinition::isSingleton).forEach(definition -> {
                    try {
                        beanContext.getBean(definition.getBeanType());
                    } catch (Exception e) {
                        // TODO: Create proper exception
                        throw new RuntimeException(
                                "Error creating bean for @RedisListener of type [" + definition.getBeanType() + "]: " + e.getMessage(), e
                        );
                    }
                });
    }

    @SuppressWarnings("unchecked")
    private static Object getExecutableMethodBean(BeanContext beanContext, BeanDefinition<?> beanDefinition,
                                                  ExecutableMethod<?, ?> method) {
        Qualifier<Object> qualifier = beanDefinition
                .getAnnotationNameByStereotype(jakarta.inject.Qualifier.class)
                .map(type -> Qualifiers.byAnnotation(beanDefinition, type))
                .orElse(null);

        Class<Object> beanType = (Class<Object>) beanDefinition.getBeanType();

        return beanContext.findBean(beanType, qualifier)
                .orElseThrow(() -> new RuntimeException("Could not find the bean to execute the method " + method));
    }

    private static Set<io.micronaut.configuration.lettuce.pubsub.MessageChannel> getMessageChannels(AnnotationValue<MessageChannel> annotationValue) {
        io.micronaut.configuration.lettuce.pubsub.MessageChannel channels = annotationValue.stringValue()
                .map(io.micronaut.configuration.lettuce.pubsub.MessageChannel::individual).orElse(null);

        return Collections.singleton(channels);
    }

}
