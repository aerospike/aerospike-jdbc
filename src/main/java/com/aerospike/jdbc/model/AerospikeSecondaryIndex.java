package com.aerospike.jdbc.model;

import com.aerospike.client.query.IndexType;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class AerospikeSecondaryIndex {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    private @interface Order {
        int value();
    }

    @Order(0)
    @JsonProperty("column_name")
    private final String binName;

    @Order(1)
    @JsonProperty("sindex_name")
    private final String indexName;

    @Order(2)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    @JsonProperty("sindex_type")
    private final IndexType indexType;

    @Order(3)
    @JsonProperty("schema_name")
    private final String namespace;

    @Order(4)
    @JsonProperty("table_name")
    private final String set;

    @Order(5)
    @JsonProperty("sindex_ratio")
    private final Integer binValuesRatio;

    public AerospikeSecondaryIndex(
            String namespace,
            String set,
            String binName,
            String indexName,
            IndexType indexType,
            Integer binValuesRatio) {
        this.namespace = namespace;
        this.set = set;
        this.binName = binName;
        this.indexName = indexName;
        this.indexType = indexType;
        this.binValuesRatio = binValuesRatio;
    }

    public String getBinName() {
        return binName;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSet() {
        return set;
    }

    public Integer getBinValuesRatio()
    {
        return binValuesRatio;
    }

    public String toKey() {
        return key(namespace, set, indexName);
    }

    public static String key(String namespace, String set, String indexName) {
        return String.format("%s/%s/%s", namespace, set, indexName);
    }

    @SuppressWarnings("all")
    public Map<String, Object> toMap() {
        ObjectMapper mapper = new ObjectMapper();
        return (Map<String, Object>) mapper.convertValue(this, Map.class);
    }

    public static List<String> getOrderedFieldNames() {
        Field[] fields = AerospikeSecondaryIndex.class.getDeclaredFields();
        Arrays.sort(fields, (o1, o2) -> {
            Order order1 = o1.getAnnotation(Order.class);
            Order order2 = o2.getAnnotation(Order.class);
            if (order1 != null && order2 != null) {
                return order1.value() - order2.value();
            } else if (order1 != null) {
                return -1;
            } else if (order2 != null) {
                return 1;
            }
            return o1.getName().compareTo(o2.getName());
        });
        return Arrays.stream(fields).map(field -> {
            JsonProperty columnName = field.getAnnotation(JsonProperty.class);
            if (columnName != null) {
                return columnName.value();
            }
            return field.getName();
        }).collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AerospikeSecondaryIndex other = (AerospikeSecondaryIndex) obj;
        return Objects.equals(binName, other.binName)
                && Objects.equals(indexName, other.indexName)
                && Objects.equals(indexType, other.indexType)
                && Objects.equals(namespace, other.namespace)
                && Objects.equals(set, other.set);
    }

    @Override
    public int hashCode() {
        return Objects.hash(binName, indexName, indexType, namespace, set);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + binName + ", " + indexName + ", " + indexType +
                ", " + namespace + ", " + set + ")";
    }
}
