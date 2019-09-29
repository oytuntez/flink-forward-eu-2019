package proofreaders.common.queue.boundary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proofreaders.common.queue.entity.Event;
import proofreaders.common.queue.entity.GenericPayload;

import java.time.Instant;
import java.util.Base64;
import java.util.Date;

import static proofreaders.common.EventType.UNKNOWN_EVENT_TYPE;

// todo ignore null values in outgoing messages (e.g kibana shows null values)
public class DefaultDeserializationSchema extends AbstractDeserializationSchema<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDeserializationSchema.class);

    private static final String EVENT = "event";
    private static final String QUEUED_AT = "_queuedAt";
    private static final String PAYLOAD = "payload";

    @Override
    public Event deserialize(byte[] message) {
        Event event = mapEvent(message);
        if (event == null) {
            try {
                event = mapEvent(Base64.getDecoder().decode(message));
            } catch (Exception e) {
                LOG.error("Error parsing event : ", e);
            }
        }
        if (event == null) {
            return mapUnknownEvent(message);
        }
        return event;
    }


    private Event mapEvent(byte[] message) {
        try {
            JsonNode root = Jackson.toJsonNode(message);
            String event = root.get(EVENT).textValue();
            Date queuedAt = new Date();

            if (root.has(QUEUED_AT)) {
                JsonNode qVal = root.get(QUEUED_AT);

                if (qVal != null && qVal.longValue() > 0) {
                    if (qVal.toString().length() == 10) {
                        queuedAt = Date.from(Instant.ofEpochSecond(qVal.longValue()));
                    } else {
                        queuedAt = Date.from(Instant.ofEpochMilli(qVal.longValue()));
                    }
                }
            }

            JsonNode payload = root.get(PAYLOAD);
            return mapEvent(event, queuedAt, payload);
        } catch (Exception e) {
            LOG.error("Error parsing event : ", e);
            return null;
        }
    }

    private Event mapEvent(String event, Date queuedAt, JsonNode payload) {
        GenericPayload genericPayload = new GenericPayload(payload);
        return new Event(event, genericPayload, queuedAt);
    }

    private Event mapUnknownEvent(byte[] message) {
        ObjectNode objectNode = Jackson.getObjectMapper().createObjectNode();
        objectNode.put(PAYLOAD, message);
        return mapEvent(UNKNOWN_EVENT_TYPE.getName(), new Date(), objectNode);
    }

}
