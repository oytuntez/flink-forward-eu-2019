package proofreaders.common.queue.controller;

import com.rabbitmq.client.Channel;
import lombok.Getter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import proofreaders.common.queue.entity.Event;

import java.io.IOException;

@SuppressWarnings("all")
@Getter
public class RabbitMqSink extends RMQSink<Event> {

    private RabbitMqPublishOptions customPublishOptions;

    public RabbitMqSink(
            RMQConnectionConfig rmqConnectionConfig,
            SerializationSchema schema,
            RMQSinkPublishOptions<Event> publishOptions
    ) {
        super(rmqConnectionConfig, schema, publishOptions);
        this.customPublishOptions = (RabbitMqPublishOptions) publishOptions;
    }

    @Override
    public void invoke(Event value) {
        byte[] msg = schema.serialize(value);

        boolean mandatory = customPublishOptions.computeMandatory(value);
        boolean immediate = customPublishOptions.computeImmediate(value);

        String rk = "events";
        if (!Strings.isNullOrEmpty(value.getTopic())) {
            rk = value.getTopic();
        }

        if (value.getTopic() != null) {
            rk = value.getTopic();
        }

        String exchange = customPublishOptions.computeExchange(value);

        try {
            channel.basicPublish(exchange, rk, mandatory, immediate,
                    customPublishOptions.computeProperties(value), msg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Channel getChannel() {
        return channel;
    }
}