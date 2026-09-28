package io.micronaut.configuration.lettuce

import io.lettuce.core.api.StatefulRedisConnection
import io.micronaut.context.BeanContext
import io.micronaut.core.type.Argument
import spock.lang.Specification

import java.util.function.Supplier

/**
 * Unit tests for the wrapper that makes a runtime connection definition win for its own type arguments
 * without becoming a candidate for raw requests.
 */
class TypedRedisConnectionBeanDefinitionSpec extends Specification {

    @SuppressWarnings(['unchecked', 'rawtypes'])
    static final Argument<StatefulRedisConnection> BYTES_CONNECTION =
            (Argument) Argument.of(StatefulRedisConnection, byte[].class, byte[].class)

    Supplier<StatefulRedisConnection> supplier = Mock(Supplier)

    TypedRedisConnectionBeanDefinition<StatefulRedisConnection> definition =
            TypedRedisConnectionBeanDefinition.of(BYTES_CONNECTION, supplier)

    void "it is primary and sorts before the default connection factories"() {
        expect:
        definition.isPrimary()
        definition.getOrder() == TypedRedisConnectionBeanDefinition.ORDER
        definition.getOrder() < 0
    }

    void "a raw request for the generic connection type is not a candidate"() {
        expect: 'the default connection keeps serving raw requests'
        !definition.isCandidateBean(Argument.of(StatefulRedisConnection))

        and: 'a null type is never a candidate'
        !definition.isCandidateBean(null)
    }

    void "a request for the matching type arguments is a candidate"() {
        expect:
        definition.isCandidateBean(BYTES_CONNECTION)
    }

    void "a request for an unrelated type is not a candidate"() {
        expect: 'String has no type parameters, so the delegate decides and rejects it'
        !definition.isCandidateBean(Argument.of(String))
    }

    void "it exposes the parameterized connection type as a singleton"() {
        expect:
        definition.isSingleton()
        definition.getBeanType() == StatefulRedisConnection
        definition.asArgument().getTypeParameters().length == 2
        definition.getTypeParameters().length == 2
        definition.getTypeArguments().size() == 2
        !definition.isAbstract()
        definition.isPresent()
        definition.load().is(definition)
    }

    void "the remaining bean definition methods are forwarded to the delegate"() {
        given:
        def delegate = io.micronaut.context.RuntimeBeanDefinition
                .builder(BYTES_CONNECTION, supplier)
                .singleton(true)
                .build()

        expect:
        definition.getScope() == delegate.getScope()
        definition.getScopeName() == delegate.getScopeName()
        definition.getExposedTypes() == delegate.getExposedTypes()
        definition.getTypeArguments(StatefulRedisConnection) == delegate.getTypeArguments(StatefulRedisConnection)
        definition.getRequiredComponents() == delegate.getRequiredComponents()
        definition.getDeclaredQualifier() == delegate.getDeclaredQualifier()
        definition.getAnnotationMetadata() == delegate.getAnnotationMetadata()
        definition.getConstructor() != null
        definition.getBeanDefinitionName().startsWith(StatefulRedisConnection.name)
        definition.getName() == StatefulRedisConnection.name
        definition.toString() != null
    }

    void "it instantiates through the supplier and is enabled"() {
        given:
        StatefulRedisConnection connection = Mock(StatefulRedisConnection)
        BeanContext beanContext = Mock(BeanContext)

        when:
        def instance = definition.instantiate(null, beanContext)

        then:
        1 * supplier.get() >> connection
        instance.is(connection)

        and:
        definition.isEnabled(beanContext, null)
    }
}
