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
