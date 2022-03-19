package io.micronaut.configuration.lettuce.pubsub.annotations;

import io.micronaut.messaging.annotation.MessageListener;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Class level annotation to indicate that a bean will be consumers of messages
 * from Redis.
 *
 * @author Cristian Morales
 * @since TODO
 */
@Documented
@Retention(RUNTIME)
@Target({ElementType.TYPE})
@MessageListener
@Inherited
public @interface RedisListener {

    String value() default "";

}
