package io.micronaut.configuration.lettuce.pubsub

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Executable
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.AnnotationMetadata
import io.micronaut.core.annotation.Introspected
import io.micronaut.core.convert.ConversionContext
import io.micronaut.core.type.Argument
import io.micronaut.core.type.Headers
import io.micronaut.core.type.MutableHeaders
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Consumes
import io.micronaut.http.annotation.Produces
import io.micronaut.http.body.TypedMessageBodyHandler
import jakarta.inject.Singleton
import spock.lang.AutoCleanup
import spock.lang.Specification

import java.nio.charset.StandardCharsets

class RedisMessageBodyHandlerSpec extends Specification {

    static final String SPEC_NAME = 'RedisMessageBodyHandlerSpec'

    @AutoCleanup
    ApplicationContext applicationContext = ApplicationContext.run(
        'redis.port': -1,
        'spec.name': SPEC_NAME
    )

    void "test consumes and produces annotations select alternative media type"() {
        given:
        def bodyHandler = applicationContext.getBean(RedisMessageBodyHandler)
        def beanDefinition = applicationContext.getBeanDefinition(TextPlainTarget)
        def writeMethod = beanDefinition.executableMethods.find { it.methodName == 'write' }
        def readMethod = beanDefinition.executableMethods.find { it.methodName == 'read' }
        def book = new Book(title: "Dune", author: "Frank Herbert")

        when:
        byte[] bytes = bodyHandler.serialize(Argument.of(Book), writeMethod.annotationMetadata, book)
        def decoded = bodyHandler.deserialize(
            bytes,
            ConversionContext.of(Argument.of(Book)),
            bodyHandler.resolveIncomingMediaType(readMethod.annotationMetadata, beanDefinition.annotationMetadata)
        ).orElseThrow()

        then:
        new String(bytes, StandardCharsets.UTF_8) == "Dune|Frank Herbert"
        decoded.title == "Dune"
        decoded.author == "Frank Herbert"
    }

    void "test default media type can be overridden via configuration"() {
        given:
        applicationContext.close()
        applicationContext = ApplicationContext.run(
            'redis.port': -1,
            'redis.pubsub.default-body-media-type': 'text/plain',
            'spec.name': SPEC_NAME
        )
        def bodyHandler = applicationContext.getBean(RedisMessageBodyHandler)
        def book = new Book(title: "Dune", author: "Frank Herbert")

        when:
        byte[] bytes = bodyHandler.serialize(Argument.of(Book), AnnotationMetadata.EMPTY_METADATA, book)
        def decoded = bodyHandler.deserialize(
            bytes,
            ConversionContext.of(Argument.of(Book)),
            bodyHandler.resolveIncomingMediaType(
                AnnotationMetadata.EMPTY_METADATA,
                AnnotationMetadata.EMPTY_METADATA
            )
        ).orElseThrow()

        then:
        new String(bytes, StandardCharsets.UTF_8) == "Dune|Frank Herbert"
        decoded.title == "Dune"
        decoded.author == "Frank Herbert"
    }

    void "test declaring type annotations select alternative media type"() {
        given:
        def bodyHandler = applicationContext.getBean(RedisMessageBodyHandler)
        def beanDefinition = applicationContext.getBeanDefinition(TypeLevelTextPlainTarget)
        def writeMethod = beanDefinition.executableMethods.find { it.methodName == 'write' }
        def readMethod = beanDefinition.executableMethods.find { it.methodName == 'read' }
        def book = new Book(title: "Dune", author: "Frank Herbert")

        when:
        byte[] bytes = bodyHandler.serialize(
            Argument.of(Book),
            writeMethod.annotationMetadata,
            beanDefinition.annotationMetadata,
            book
        )
        def decoded = bodyHandler.deserialize(
            bytes,
            ConversionContext.of(Argument.of(Book)),
            bodyHandler.resolveIncomingMediaType(readMethod.annotationMetadata, beanDefinition.annotationMetadata)
        ).orElseThrow()

        then:
        new String(bytes, StandardCharsets.UTF_8) == "Dune|Frank Herbert"
        decoded.title == "Dune"
        decoded.author == "Frank Herbert"
    }

    void "test serialize null values returns empty bytes for all overloads"() {
        given:
        def bodyHandler = applicationContext.getBean(RedisMessageBodyHandler)

        expect:
        bodyHandler.serialize(null).length == 0
        bodyHandler.serialize(Argument.of(Book), MediaType.APPLICATION_JSON_TYPE, null).length == 0
        bodyHandler.serialize(Argument.of(Book), AnnotationMetadata.EMPTY_METADATA, null).length == 0
        bodyHandler.serialize(
            Argument.of(Book),
            AnnotationMetadata.EMPTY_METADATA,
            AnnotationMetadata.EMPTY_METADATA,
            null
        ).length == 0
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    static class TextPlainTarget {

        @Produces(MediaType.TEXT_PLAIN)
        @Executable
        void write(Book book) {
        }

        @Consumes(MediaType.TEXT_PLAIN)
        @Executable
        void read(Book book) {
        }
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    static class TypeLevelTextPlainTarget {

        @Executable
        void write(Book book) {
        }

        @Executable
        void read(Book book) {
        }
    }

    @Singleton
    @Requires(property = 'spec.name', value = SPEC_NAME)
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    static class BookTextPlainBodyHandler implements TypedMessageBodyHandler<Book> {

        @Override
        Argument<Book> getType() {
            return Argument.of(Book)
        }

        @Override
        Book read(Argument<Book> type, MediaType mediaType, Headers httpHeaders, InputStream inputStream) {
            def parts = inputStream.getText(StandardCharsets.UTF_8.name()).split('\\|', 2)
            return new Book(title: parts[0], author: parts[1])
        }

        @Override
        void writeTo(Argument<Book> type, MediaType mediaType, Book object, MutableHeaders outgoingHeaders, OutputStream outputStream) {
            outputStream.write("${object.title}|${object.author}".getBytes(StandardCharsets.UTF_8))
        }
    }

    @Introspected
    static class Book {
        String title
        String author
    }
}
