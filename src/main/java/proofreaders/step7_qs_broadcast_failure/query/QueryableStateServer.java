package proofreaders.step7_qs_broadcast_failure.query;

import io.javalin.Javalin;
import io.javalin.core.JavalinConfig;
import io.javalin.http.Context;
import org.atteo.classindex.ClassIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proofreaders.step7_qs_broadcast_failure.annotations.QueryableStateEndpoint;

import java.lang.reflect.Method;

import static io.javalin.apibuilder.ApiBuilder.path;
import static io.javalin.apibuilder.ApiBuilder.post;

public class QueryableStateServer {
    public static Logger log = LoggerFactory.getLogger(QueryableStateServer.class);
    public static String QUERY_METHOD_NAME = "query";

    public static void main(String[] args) {
        Javalin.create(QueryableStateServer::prepareConfig)
                .routes(QueryableStateServer::prepareRoutes)
                .start(7000);
    }

    private static void prepareConfig(JavalinConfig config) {
        config.defaultContentType = "application/json";
        config.enableCorsForAllOrigins();
        //config.enableDevLogging();
    }

    private static void prepareRoutes() {
        Iterable<Class<?>> klasses = ClassIndex.getAnnotated(QueryableStateEndpoint.class);

        for (Class<?> klass : klasses) {
            Object instance;
            Method method;

            log.debug("Processing QueryableStateEndpoint annotation for " + klass.getName());

            try {
                instance = klass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
                continue;
            }

            QueryableStateEndpoint annotation = klass.getAnnotation(QueryableStateEndpoint.class);
            if (annotation == null || annotation.endpoint().length() < 1) {
                log.debug("QueryableStateEndpoint is not given correctly for " + klass.getName());
                continue;
            }

            try {
                method = klass.getMethod(QUERY_METHOD_NAME, Context.class);
            } catch (NoSuchMethodException e) {
                log.warn(klass.getName() + " does not provide a `query` method for QueryableStateServer.");
                continue;
            }

            log.info("Adding endpoint " + annotation.endpoint() + " from " + klass.getName());

            path(annotation.endpoint(), () -> {
                post((Context ctx) -> method.invoke(instance, ctx));
            });
        }
    }
}