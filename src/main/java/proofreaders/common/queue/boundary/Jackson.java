package proofreaders.common.queue.boundary;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

public final class Jackson {

    private static final Map<Class<? extends Module>, Module> _MODULES = new HashMap<>();
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        setup();
    }

    private Jackson() {
    }

    private static final Jackson _INSTANCE = new Jackson();

    private static synchronized void setup() {
        OBJECT_MAPPER = new ObjectMapper().setDateFormat(new SimpleDateFormat("dd-MM-yyyy hh:mm:ss"))
                                          .registerModules(_MODULES.values())
                                          .registerModule(new JavaTimeModule())
                                          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                                          .enable(SerializationFeature.INDENT_OUTPUT)
                                          .disable(FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static synchronized Jackson registerModule(Module module) {
        _MODULES.put(module.getClass(), module);
        setup();
        return _INSTANCE;
    }

    public static synchronized Jackson unregisterModule(Module module) {
        _MODULES.remove(module.getClass());
        setup();
        return _INSTANCE;
    }

    public static synchronized ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    public static <T> T fromString(String string, Class<T> clazz) {
        try {
            return getObjectMapper().readValue(string, clazz);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "The given string value: " + string + " cannot be transformed to Json object"
            );
        }
    }

    public static String toString(Object value) {
        try {
            return getObjectMapper().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "The given Json object value: " + value + " cannot be transformed to a String"
            );
        }
    }

    public static JsonNode toJsonNode(String value) {
        try {
            return getObjectMapper().readTree(value);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static JsonNode toJsonNode(byte[] value) {
        try {
            return getObjectMapper().readTree(value);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T clone(T value) {
        return fromString(toString(value), (Class<T>) value.getClass());
    }


}
