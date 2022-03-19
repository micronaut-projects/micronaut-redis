package io.micronaut.configuration.lettuce.pubsub.annotations;

import io.micronaut.context.annotation.Executable;
import io.micronaut.core.bind.annotation.Bindable;
import io.micronaut.messaging.annotation.MessageMapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to specify which message channel to subscribe to and mark the handler method
 *
 * @author Cristian Morales
 * @since TODO
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER, ElementType.TYPE})
@Bindable
@Inherited
@Executable
@MessageMapping
public @interface MessageChannel {

    String value() default "";

    String[] channels() default {};

    String[] patterns() default {};

}
