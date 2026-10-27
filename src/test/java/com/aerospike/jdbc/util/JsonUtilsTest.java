package com.aerospike.jdbc.util;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class JsonUtilsTest {

    @Test
    public void testIsCdt() {
        assertTrue(JsonUtils.isCdt(Collections.emptyMap()));
        assertTrue(JsonUtils.isCdt(Collections.emptyList()));
        assertFalse(JsonUtils.isCdt("str"));
        assertFalse(JsonUtils.isCdt(1L));
        assertFalse(JsonUtils.isCdt(null));
    }

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
    public void testNonStringKeysToJson() {
        Map<Object, Object> map = new TreeMap<>();
        map.put(1L, "one");
        map.put(2L, "two");
        assertEquals(JsonUtils.toJson(map), "{\"1\":\"one\",\"2\":\"two\"}");

        Map<Object, Object> blobKeyMap = Collections.singletonMap(new byte[]{1, 2, 3}, 1L);
        assertEquals(JsonUtils.toJson(blobKeyMap), "{\"AQID\":1}");
    }
}
