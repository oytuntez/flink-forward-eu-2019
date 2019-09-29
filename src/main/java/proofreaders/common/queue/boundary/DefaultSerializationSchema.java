package proofreaders.common.queue.boundary;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class DefaultSerializationSchema implements SerializationSchema<Object> {
    private static ObjectMapper mapper = new ObjectMapper();

    public DefaultSerializationSchema() {
        Jackson.getObjectMapper().enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public byte[] serialize(Object element) {
        try {
            return mapper.writeValueAsString(element).getBytes();
        } catch (JsonProcessingException e) {
            return null;
        }
    }

}
