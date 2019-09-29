package proofreaders.common.queue.controller;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.Serializable;
import java.util.UUID;

@Getter
@Setter
public class RabbitMqConnectionConfig implements Serializable {

    public static final String RABBIT_EXCHANGE = "RABBIT_EXCHANGE";
    public static final String RABBIT_DLX_EXCHANGE = "RABBIT_DLX_EXCHANGE";
    public static final String RABBIT_ROUTING = "RABBIT_ROUTING";
    public static final String RABBIT_DLX_ROUTING = "RABBIT_DLX_ROUTING";
    public static final String RABBIT_HOST = "RABBIT_HOST";
    public static final String RABBIT_PORT = "RABBIT_PORT";
    public static final String RABBIT_QUEUE = "RABBIT_QUEUE";
    public static final String RABBIT_DLX_QUEUE = "RABBIT_DLX_QUEUE";
    public static final String RABBIT_USER = "RABBIT_USER";
    public static final String RABBIT_PASSWORD = "RABBIT_PASSWORD";
    public static final String RABBIT_VIRTUAL_HOST = "RABBIT_VIRTUAL_HOST";

    private String rabbit_exchange;
    private String rabbit_routing;
    private String rabbit_host;
    private Integer rabbit_port;
    private String queue;
    private String rabbit_user;
    private String rabbit_password;
    private String rabbit_virtual_host;

    public RabbitMqConnectionConfig() {
        queue = UUID.randomUUID().toString();
    }

    public RabbitMqConnectionConfig(
            String rabbit_exchange,
            String rabbit_routing,
            String rabbit_host,
            Integer rabbit_port,
            String rabbit_queue,
            String rabbit_user,
            String rabbit_password,
            String rabbit_virtual_host
    ) {
        this.rabbit_exchange = rabbit_exchange;
        this.rabbit_routing = rabbit_routing;
        this.rabbit_host = rabbit_host;
        this.rabbit_port = rabbit_port;
        this.queue = rabbit_queue;
        this.rabbit_user = rabbit_user;
        this.rabbit_password = rabbit_password;
        this.rabbit_virtual_host = rabbit_virtual_host;
    }

    public RabbitMqConnectionConfig(ParameterTool parameterTool) {
        this(
                parameterTool.get(RABBIT_EXCHANGE),
                parameterTool.get(RABBIT_ROUTING),
                parameterTool.get(RABBIT_HOST),
                parameterTool.getInt(RABBIT_PORT),
                parameterTool.get(RABBIT_QUEUE),
                parameterTool.get(RABBIT_USER),
                parameterTool.get(RABBIT_PASSWORD),
                parameterTool.get(RABBIT_VIRTUAL_HOST)
        );
    }

    public RMQConnectionConfig build() {
        return new RMQConnectionConfig.Builder()
                .setHost(getRabbit_host())
                .setPort(getRabbit_port())
                .setUserName(getRabbit_user())
                .setPassword(getRabbit_password())
                .setVirtualHost(getRabbit_virtual_host())
                .build();
    }


}
