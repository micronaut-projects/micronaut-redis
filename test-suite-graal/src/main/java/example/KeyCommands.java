package example;

import io.lettuce.core.dynamic.Commands;

public interface KeyCommands extends Commands {

    String get(String key);

    String set(String key, String value);
}