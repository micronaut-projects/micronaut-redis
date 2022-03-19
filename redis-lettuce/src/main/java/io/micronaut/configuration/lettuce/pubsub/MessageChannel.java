package io.micronaut.configuration.lettuce.pubsub;

import java.io.Serializable;
import java.util.Objects;

public class MessageChannel implements Serializable {

    private final String value;

    public MessageChannel(String value) {
        this.value = value;
    }

    public static MessageChannel individual(String name) {
        return new MessageChannel(name);
    }

    public static MessageChannel pattern(String pattern) {
        return new MessageChannel(pattern);
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageChannel that = (MessageChannel) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

}
