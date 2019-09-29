package proofreaders.common.queue.boundary;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import proofreaders.common.queue.controller.RabbitMqConnectionConfig;
import proofreaders.common.queue.controller.RabbitMqPublishOptions;
import proofreaders.common.queue.controller.RabbitMqSink;

public class DefaultSinkBuilder {

    private RabbitMqConnectionConfig rabbitMqConfig;
    private SerializationSchema serializationSchema = new SimpleStringSchema();
    private ParameterTool parameterTool;

    public DefaultSinkBuilder rabbitMqConfig(ParameterTool parameterTool) {
        this.rabbitMqConfig = new RabbitMqConnectionConfig(parameterTool);
        this.parameterTool = parameterTool;
        return this;
    }

    public DefaultSinkBuilder setSerializationSchema(SerializationSchema serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    public RabbitMqSink buildDefaultSink() {
        if (rabbitMqConfig == null) {
            throw new RuntimeException("Queue connection config is invalid!");
        }

        RMQConnectionConfig config = rabbitMqConfig.build();
        return new RabbitMqSink(
                config,
                serializationSchema,
                new RabbitMqPublishOptions(parameterTool)
        );
    }

}
