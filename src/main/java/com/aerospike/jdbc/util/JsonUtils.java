package com.aerospike.jdbc.util;

import com.aerospike.client.Value;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.logging.Logger;

/**
 * Serializes Aerospike collection data type (CDT) values to JSON.
 * <p>
 * Conversions:
 * <ul>
 *     <li>blob values and HLL values are Base64-encoded strings</li>
 *     <li>GeoJSON values are embedded as JSON objects, keeping their original number text</li>
 *     <li>non-string map keys are converted to strings: integers and doubles to their decimal
 *     representation, blobs to Base64</li>
 * </ul>
 */
public final class JsonUtils {

    private static final Logger logger = Logger.getLogger(JsonUtils.class.getName());

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new SimpleModule()
                .addSerializer(Value.GeoJSONValue.class, new JsonSerializer<Value.GeoJSONValue>() {
                    @Override
                    public void serialize(Value.GeoJSONValue value, JsonGenerator gen,
                                          SerializerProvider serializers) throws IOException {
                        String text = value.toString();
                        String json = compactJson(text);
                        if (json != null) {
                            gen.writeRawValue(json);
                        } else {
                            gen.writeString(text);
                        }
                    }
                })
                .addSerializer(Value.HLLValue.class, new JsonSerializer<Value.HLLValue>() {
                    @Override
                    public void serialize(Value.HLLValue value, JsonGenerator gen,
                                          SerializerProvider serializers) throws IOException {
                        gen.writeBinary(value.getBytes());
                    }
                })
                // blob keys of unordered maps are byte[]; the default key serializer would print "[B@..."
                .addKeySerializer(byte[].class, new JsonSerializer<byte[]>() {
                    @Override
                    public void serialize(byte[] value, JsonGenerator gen, SerializerProvider serializers)
                            throws IOException {
                        gen.writeFieldName(Base64.getEncoder().encodeToString(value));
                    }
                })
                // blob keys of key-ordered maps are wrapped in ByteBuffer by the client
                .addKeySerializer(ByteBuffer.class, new JsonSerializer<ByteBuffer>() {
                    @Override
                    public void serialize(ByteBuffer value, JsonGenerator gen, SerializerProvider serializers)
                            throws IOException {
                        gen.writeFieldName(Base64.getEncoder().encodeToString(toBytes(value)));
                    }
                }));
    }

    private JsonUtils() {
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

    /**
     * Validates a single JSON value and removes insignificant whitespace, keeping the original
     * number text (a tree round trip would rewrite {@code 1e10} as {@code 1.0E10} and could round decimals).
     *
     * @return the compact JSON, or {@code null} if the text is not exactly one valid JSON value
     */
    private static String compactJson(String text) {
        StringWriter out = new StringWriter();
        try (JsonParser parser = mapper.getFactory().createParser(text);
             JsonGenerator tmp = mapper.getFactory().createGenerator(out)) {
            JsonToken token = parser.nextToken();
            if (token == null) {
                return null;
            }
            int depth = 0;
            do {
                if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                    tmp.writeNumber(parser.getText());
                } else {
                    tmp.copyCurrentEvent(parser);
                }
                if (token.isStructStart()) {
                    depth++;
                } else if (token.isStructEnd()) {
                    depth--;
                }
            } while (depth > 0 && (token = parser.nextToken()) != null);
            if (depth != 0 || parser.nextToken() != null) {
                return null;
            }
        } catch (IOException e) {
            return null;
        }
        return out.toString();
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
