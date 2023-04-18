package example;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.dynamic.RedisCommandFactory;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller("/")
public class RedisController {

    private final StatefulRedisConnection<String, String> connection;
    private final KeyCommands keyCommands;

    public RedisController(StatefulRedisConnection<String, String> connection) {
        this.connection = connection;
        RedisCommandFactory factory = new RedisCommandFactory(connection);
        this.keyCommands = factory.getCommands(KeyCommands.class);
    }

    @Get("/set")
    public void setKey() {
        connection.sync().set("key", "Hello World");
    }

    @Get("/get")
    public String getKey() {
        return connection.sync().get("key");
    }

    @Get("/command-set")
    public void keyCommandSet() {
        keyCommands.set("key2", "Hello World");
    }

    @Get("/command-get")
    public String keyCommandGet() {
        return keyCommands.get("key2");
    }
}
