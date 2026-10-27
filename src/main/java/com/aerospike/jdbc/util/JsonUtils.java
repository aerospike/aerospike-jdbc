package com.aerospike.jdbc.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public final class JsonUtils {

    private static final Logger logger = Logger.getLogger(JsonUtils.class.getName());

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new SimpleModule()
                    // Aerospike maps may have blob keys; the default key serializer would print "[B@..."
                    .addKeySerializer(byte[].class, new JsonSerializer<byte[]>() {
                        @Override
                        public void serialize(byte[] value, JsonGenerator gen, SerializerProvider serializers)
                                throws IOException {
                            gen.writeFieldName(Base64.getEncoder().encodeToString(value));
                        }
                    }));

    private JsonUtils() {
    }

    /**
     * Returns true if the value is a collection data type (map or list) that should be rendered as JSON.
     */
    public static boolean isCdt(Object value) {
        return value instanceof Map<?, ?> || value instanceof List<?>;
    }

    /**
     * Serializes a map or list bin value to a JSON string.
     * Falls back to {@link Object#toString()} if the value cannot be serialized.
     */
    public static String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            logger.warning(() -> "Failed to serialize value to JSON: " + e.getMessage());
            return value.toString();
        }
    }
}
