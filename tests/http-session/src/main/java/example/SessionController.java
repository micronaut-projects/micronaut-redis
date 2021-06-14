package example;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.session.Session;

@Controller("/sessions")
public class SessionController {

    @Get
    String simple(Session session) {
        return (String) session.get("myValue").orElseGet(() -> {
            session.put("myValue", "value in session");
            return "not in session";
        });
    }

}
