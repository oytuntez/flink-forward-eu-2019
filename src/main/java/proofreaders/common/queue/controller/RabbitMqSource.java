package proofreaders.common.queue.controller;

import com.rabbitmq.client.AMQP;
import lombok.Setter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import proofreaders.common.queue.entity.Event;

import java.io.IOException;

@SuppressWarnings("all")
@Setter
public class RabbitMqSource extends RMQSource<Event> {

    private String exchange;
    private String routing;
    private boolean queueExists;

    public RabbitMqSource(
            RMQConnectionConfig rmqConnectionConfig,
            String queueName,
            DeserializationSchema deserializationSchema
    ) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
    }

    @Override
    protected void setupQueue() throws IOException {
        channel.basicQos(0);
        AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queueName);
        channel.queueBind(declareOk.getQueue(), exchange, routing);
    }
}
