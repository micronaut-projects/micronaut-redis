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
package io.micronaut.configuration.lettuce;

import io.micronaut.context.BeanContext;
import io.micronaut.context.BeanResolutionContext;
import io.micronaut.context.Qualifier;
import io.micronaut.context.RuntimeBeanDefinition;
import io.micronaut.context.exceptions.BeanInstantiationException;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.ConstructorInjectionPoint;
import org.jspecify.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A singleton connection bean registered for the concrete key and value types of an unqualified
 * {@link io.lettuce.core.codec.RedisCodec} bean.
 *
 * <p>The default connection factories produce generic {@code StatefulRedisConnection<K, V>} beans that are
 * candidates for every parameterization, and the standalone connection is {@code @Primary}, so a request for
 * {@code StatefulRedisConnection<byte[], byte[]>} would resolve the default connection and fail with the wrong
 * codec. This definition is {@code @Primary} with a higher precedence than the default connection so that it
 * wins whenever the requested type arguments match its own, and it is never a candidate for a raw request,
 * which the default connection continues to serve.</p>
 *
 * @param <T> The connection type
 * @since 7.2.0
 */
final class TypedRedisConnectionBeanDefinition<T> implements RuntimeBeanDefinition<T> {

    /**
     * Sorts before the default connection factory beans, which have the default order of zero.
     */
    static final int ORDER = -100;

    private final RuntimeBeanDefinition<T> delegate;

    private TypedRedisConnectionBeanDefinition(RuntimeBeanDefinition<T> delegate) {
        this.delegate = delegate;
    }

    /**
     * Creates a singleton connection definition for the given parameterized connection type.
     *
     * @param beanType The connection type with its key and value type arguments
     * @param connectionSupplier Creates the connection
     * @param <T> The connection type
     * @return The definition
     */
    static <T> TypedRedisConnectionBeanDefinition<T> of(Argument<T> beanType, Supplier<T> connectionSupplier) {
        return new TypedRedisConnectionBeanDefinition<>(
            RuntimeBeanDefinition.builder(beanType, connectionSupplier)
                .singleton(true)
                .build()
        );
    }

    @Override
    public boolean isCandidateBean(@Nullable Argument<?> beanType) {
        if (beanType == null) {
            return false;
        }
        if (beanType.getTypeParameters().length == 0 && beanType.getType().getTypeParameters().length > 0) {
            // a raw request for a generic connection type is served by the default connection
            return false;
        }
        return delegate.isCandidateBean(beanType);
    }

    @Override
    public boolean isPrimary() {
        return true;
    }

    @Override
    public int getOrder() {
        return ORDER;
    }

    @Override
    public Class<T> getBeanType() {
        return delegate.getBeanType();
    }

    @Override
    public T instantiate(BeanResolutionContext resolutionContext, BeanContext context) throws BeanInstantiationException {
        return delegate.instantiate(resolutionContext, context);
    }

    @Override
    public boolean isEnabled(BeanContext context, @Nullable BeanResolutionContext resolutionContext) {
        return delegate.isEnabled(context, resolutionContext);
    }

    @Override
    public List<Argument<?>> getTypeArguments(Class<?> type) {
        return delegate.getTypeArguments(type);
    }

    @Override
    public List<Argument<?>> getTypeArguments() {
        return delegate.getTypeArguments();
    }

    @Override
    public Class<?>[] getTypeParameters() {
        return delegate.getTypeParameters();
    }

    @Override
    public Argument<T> asArgument() {
        return delegate.asArgument();
    }

    @Override
    public boolean isAbstract() {
        return delegate.isAbstract();
    }

    @Override
    public boolean isSingleton() {
        return delegate.isSingleton();
    }

    @Override
    public Optional<Class<? extends Annotation>> getScope() {
        return delegate.getScope();
    }

    @Override
    public Optional<String> getScopeName() {
        return delegate.getScopeName();
    }

    @Override
    public Set<Class<?>> getExposedTypes() {
        return delegate.getExposedTypes();
    }

    @Override
    public @Nullable Qualifier<T> getDeclaredQualifier() {
        return delegate.getDeclaredQualifier();
    }

    @Override
    public String getBeanDefinitionName() {
        return delegate.getBeanDefinitionName();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public AnnotationMetadata getAnnotationMetadata() {
        return delegate.getAnnotationMetadata();
    }

    @Override
    public BeanDefinition<T> load() {
        return this;
    }

    @Override
    public boolean isPresent() {
        return delegate.isPresent();
    }

    @Override
    public ConstructorInjectionPoint<T> getConstructor() {
        return delegate.getConstructor();
    }

    @Override
    public Collection<Class<?>> getRequiredComponents() {
        return delegate.getRequiredComponents();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
