package proofreaders.common.queue.boundary;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import proofreaders.common.queue.controller.RabbitMqConnectionConfig;
import proofreaders.common.queue.controller.RabbitMqSource;
import proofreaders.common.queue.entity.Event;

import java.util.Date;

public class DefaultSourceBuilder {

    private RabbitMqConnectionConfig rabbitMqConfig;
    private DeserializationSchema deserializationSchema = new SimpleStringSchema();
    protected static ParameterTool parameterTool = new Config().getConfiguration();

    public DefaultSourceBuilder rabbitMqConfig(RabbitMqConnectionConfig rabbitMqConfig) {
        this.rabbitMqConfig = rabbitMqConfig;
        return this;
    }

    public DefaultSourceBuilder setDeserializationSchema(DeserializationSchema deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    public RabbitMqSource build() {
        if (rabbitMqConfig == null) {
            throw new RuntimeException("Queue connection config is invalid!");
        }

        RMQConnectionConfig config = new RMQConnectionConfig.Builder()
                .setHost(rabbitMqConfig.getRabbit_host())
                .setPort(rabbitMqConfig.getRabbit_port())
                .setUserName(rabbitMqConfig.getRabbit_user())
                .setPassword(rabbitMqConfig.getRabbit_password())
                .setVirtualHost(rabbitMqConfig.getRabbit_virtual_host())
                .setAutomaticRecovery(true)
                .build();

        RabbitMqSource rabbitMqSource = new RabbitMqSource(config, rabbitMqConfig.getQueue(), deserializationSchema);
        rabbitMqSource.setExchange(rabbitMqConfig.getRabbit_exchange());
        rabbitMqSource.setRouting(rabbitMqConfig.getRabbit_routing());
        return rabbitMqSource;
    }

    public static SingleOutputStreamOperator<Event> buildSource(
            StreamExecutionEnvironment environment,
            String exchange,
            String route,
            String queue
    ) {
        // RabbitMq source config
        RabbitMqConnectionConfig rabbitMqConnectionConfig = new RabbitMqConnectionConfig(parameterTool);
        if (!Strings.isNullOrEmpty(exchange)) {
            rabbitMqConnectionConfig.setRabbit_exchange(exchange);
        }
        if (!Strings.isNullOrEmpty(route)) {
            rabbitMqConnectionConfig.setRabbit_routing(route);
        }
        if (!Strings.isNullOrEmpty(queue)) {
            rabbitMqConnectionConfig.setQueue(queue);
        }
        RabbitMqSource source = new DefaultSourceBuilder().rabbitMqConfig(rabbitMqConnectionConfig)
                .setDeserializationSchema(new DefaultDeserializationSchema())
                .build();

        DataStreamSource<Event> defaultStream = environment.addSource(source, "RabbitMQ Event Source - " + queue);

        return defaultStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
                    @Override
                    public long extractAscendingTimestamp(Event element) {
                        Date queuedAt = element.getQueuedAt();
                        long time = queuedAt != null ? queuedAt.getTime() : System.currentTimeMillis();
                        return time > 1 ? time : System.currentTimeMillis();
                    }
                });
    }

    public static SingleOutputStreamOperator<Event> buildDefaultSource(StreamExecutionEnvironment environment) {
        return buildSource(environment, null, null, null);
    }

}
