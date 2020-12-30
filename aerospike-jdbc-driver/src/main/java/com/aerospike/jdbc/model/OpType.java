package com.aerospike.jdbc.model;

public enum OpType {
    EQUALS,
    LESS,
    LESS_EQUALS,
    GREATER,
    GREATER_EQUALS,
    LIKE,
    NULL,
    NOT_NULL,
    NOT,
    OR,
    AND,
    UNKNOWN;

    public static OpType fromOperator(String op) {
        switch (op.toUpperCase()) {
            case "=":
                return EQUALS;
            case "<":
                return LESS;
            case "<=":
                return LESS_EQUALS;
            case ">":
                return GREATER;
            case ">=":
                return GREATER_EQUALS;
            case "OR":
                return OR;
            case "AND":
                return AND;
        }
        return UNKNOWN;
    }
}
