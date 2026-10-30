package com.aerospike.jdbc.util;

import com.aerospike.client.Value;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;

public class JsonUtilsTest {

    @Test
    public void testNestedMapToJson() {
        Map<String, Object> map = Collections.singletonMap("foo",
                Collections.singletonMap("bar", Collections.singletonMap("baz", 1L)));
        assertEquals(JsonUtils.toJson(map), "{\"foo\":{\"bar\":{\"baz\":1}}}");
    }

    @Test
    public void testMixedValuesToJson() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("str", "a\"b");
        map.put("list", Arrays.asList("x", 2L, 3.5, true, null));
        map.put("bytes", new byte[]{1, 2, 3});
        assertEquals(JsonUtils.toJson(map),
                "{\"str\":\"a\\\"b\",\"list\":[\"x\",2,3.5,true,null],\"bytes\":\"AQID\"}");
    }

    @Test
    public void testGeoJsonValueToJson() {
        Map<String, Object> map = Collections.singletonMap("loc",
                Value.getAsGeoJSON("{\"type\": \"Point\", \"coordinates\": [-122.0, 37.5]}"));
        assertEquals(JsonUtils.toJson(map),
                "{\"loc\":{\"type\":\"Point\",\"coordinates\":[-122.0,37.5]}}");
    }

    @Test
    public void testGeoJsonNumberTextIsPreserved() {
        Map<String, Object> map = Collections.singletonMap("loc",
                Value.getAsGeoJSON("{\"type\": \"Point\", \"coordinates\": [1e10, 37.123456789012345]}"));
        assertEquals(JsonUtils.toJson(map),
                "{\"loc\":{\"type\":\"Point\",\"coordinates\":[1e10,37.123456789012345]}}");
    }

    @Test
    public void testInvalidGeoJsonFallsBackToString() {
        assertEquals(JsonUtils.toJson(Collections.singletonMap("loc",
                        Value.getAsGeoJSON("{\"type\":\"Point\"} x"))),
                "{\"loc\":\"{\\\"type\\\":\\\"Point\\\"} x\"}");
        assertEquals(JsonUtils.toJson(Collections.singletonMap("loc", Value.getAsGeoJSON("{} {}"))),
                "{\"loc\":\"{} {}\"}");
        assertEquals(JsonUtils.toJson(Collections.singletonMap("loc", Value.getAsGeoJSON("not json"))),
                "{\"loc\":\"not json\"}");
    }

    @Test
    public void testGeoJsonValuesInListToJson() {
        Map<String, Object> map = Collections.singletonMap("a", Arrays.asList(
                Value.getAsGeoJSON("{\"type\":\"Point\",\"coordinates\":[1,2]}"),
                Value.getAsGeoJSON("{\"type\":\"Point\",\"coordinates\":[3,4]}")));
        assertEquals(JsonUtils.toJson(map),
                "{\"a\":[{\"type\":\"Point\",\"coordinates\":[1,2]},{\"type\":\"Point\",\"coordinates\":[3,4]}]}");
    }

    @Test
    public void testGeoJsonStringPropertiesToJson() {
        Map<String, Object> map = Collections.singletonMap("loc", Value.getAsGeoJSON(
                "{\"type\":\"Feature\",\"properties\":{\"name\":\"Z\u00fcrich \\\"HQ\\\" \\u00e9\\n\"}}"));
        assertEquals(JsonUtils.toJson(map),
                "{\"loc\":{\"type\":\"Feature\",\"properties\":{\"name\":\"Z\u00fcrich \\\"HQ\\\" \u00e9\\n\"}}}");
    }

    @Test
    public void testSpecialNumbersToJson() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("doubles", Arrays.asList(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
        map.put("max", Long.MAX_VALUE);
        map.put("min", Long.MIN_VALUE);
        assertEquals(JsonUtils.toJson(map),
                "{\"doubles\":[\"NaN\",\"Infinity\",\"-Infinity\"],"
                        + "\"max\":9223372036854775807,\"min\":-9223372036854775808}");
    }

    @Test
    public void testHllValueToJson() {
        Map<String, Object> map = Collections.singletonMap("hll", Value.getAsHLL(new byte[]{1, 2, 3}));
        assertEquals(JsonUtils.toJson(map), "{\"hll\":\"AQID\"}");
    }

    @Test
    public void testNonStringKeysToJson() {
        Map<Object, Object> longKeyMap = new TreeMap<>();
        longKeyMap.put(1L, "one");
        longKeyMap.put(2L, "two");
        assertEquals(JsonUtils.toJson(longKeyMap), "{\"1\":\"one\",\"2\":\"two\"}");

        Map<Object, Object> doubleKeyMap = Collections.singletonMap(1.5, "x");
        assertEquals(JsonUtils.toJson(doubleKeyMap), "{\"1.5\":\"x\"}");
    }

    @Test
    public void testBlobKeysToJson() {
        // unordered maps return blob keys as byte[]
        Map<Object, Object> blobKeyMap = Collections.singletonMap(new byte[]{1, 2, 3}, 1L);
        assertEquals(JsonUtils.toJson(blobKeyMap), "{\"AQID\":1}");

        // key-ordered maps return blob keys wrapped in ByteBuffer
        Map<Object, Object> wrappedKeyMap = new TreeMap<>();
        ByteBuffer key = ByteBuffer.wrap(new byte[]{1, 2, 3});
        wrappedKeyMap.put(key, 1L);
        assertEquals(JsonUtils.toJson(wrappedKeyMap), "{\"AQID\":1}");
        assertEquals(key.remaining(), 3); // the key buffer is not consumed
    }

    @Test
    public void testMapEntryListToJson() {
        // index/rank range results are returned as a list of entries
        Object entries = Collections.singletonList(new AbstractMap.SimpleEntry<>("k", 1L));
        assertEquals(JsonUtils.toJson(entries), "[{\"k\":1}]");
    }
}
