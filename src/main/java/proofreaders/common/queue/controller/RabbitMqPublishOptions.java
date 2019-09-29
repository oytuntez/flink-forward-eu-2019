package proofreaders.common.queue.controller;

import com.rabbitmq.client.AMQP;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import proofreaders.common.queue.entity.Event;

import java.io.Serializable;

// todo not sure if we need these low level customizations here in its own extended class
public class RabbitMqPublishOptions implements RMQSinkPublishOptions<Event>, Serializable {

    private RabbitMqConnectionConfig connectionConfig;

    public RabbitMqPublishOptions(ParameterTool parameterTool) {
        connectionConfig = new RabbitMqConnectionConfig(parameterTool);
    }

    @Override
    public boolean computeMandatory(Event a) {
        return true;
    }

    @Override
    public String computeRoutingKey(Event a) {
        return connectionConfig.getRabbit_routing();
    }

    @Override
    public AMQP.BasicProperties computeProperties(Event a) {
        return null;
    }

    @Override
    public String computeExchange(Event a) {
        return connectionConfig.getRabbit_exchange();
    }

}
