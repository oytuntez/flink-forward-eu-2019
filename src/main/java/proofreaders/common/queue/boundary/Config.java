package proofreaders.common.queue.boundary;

import lombok.Getter;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static proofreaders.common.queue.controller.RabbitMqConnectionConfig.*;

@Getter
public class Config {

    private Map<String, String> defaultConfig = new HashMap<>();
    private ParameterTool configuration;
    public static final String QUERYABLE_SERVER_HOST = "QUERYABLE_SERVER_HOST";

    public Config() {
        readEnvironment();
    }

    public Config(String[] args) {
        readEnvironment();
        configuration.mergeWith(ParameterTool.fromArgs(args));
    }

    public Config(File configFile) throws IOException {
        readEnvironment();
        configuration.mergeWith(ParameterTool.fromPropertiesFile(configFile));
    }

    private void readEnvironment() {
        defaultConfig.put(RABBIT_EXCHANGE, "default-exchange");
        defaultConfig.put(RABBIT_DLX_EXCHANGE, "dead-letter-exchange");
        defaultConfig.put(RABBIT_ROUTING, "events.#");
        defaultConfig.put(RABBIT_DLX_ROUTING, "#");
        defaultConfig.put(RABBIT_HOST, "localhost");
        defaultConfig.put(RABBIT_PORT, "5672");
        defaultConfig.put(RABBIT_USER, "ipm");
        defaultConfig.put(RABBIT_QUEUE, "ipm-events");
        defaultConfig.put(RABBIT_DLX_QUEUE, "dlx-keeper");
        defaultConfig.put(RABBIT_PASSWORD, "54VKsYM1Vh29sT");
        defaultConfig.put(RABBIT_VIRTUAL_HOST, "/");
        defaultConfig.put(QUERYABLE_SERVER_HOST, "127.0.0.1");

        Map<String, String> envConfig = new HashMap<>(System.getenv());
        defaultConfig.forEach(envConfig::putIfAbsent);

        configuration = ParameterTool.fromMap(envConfig);
    }

    public ParameterTool getConfiguration() {
        return configuration;
    }

}
