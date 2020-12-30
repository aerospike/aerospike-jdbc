package com.aerospike.jdbc.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class WhereExpression {

    private List<WhereExpression> inner;

    private String column;
    private OpType opType;
    private Object value;

    public WhereExpression() {
        inner = new ArrayList<>();
    }

    public WhereExpression(String column, OpType opType, Object value) {
        super();
        this.column = column;
        this.opType = opType;
        this.value = value;
    }

    public WhereExpression(OpType opType, String column) {
        super();
        this.column = column;
        this.opType = opType;
    }

    public void append(WhereExpression exp) {
        inner.add(exp);
    }

    public boolean isWrapper() {
        return Objects.isNull(column) && Objects.isNull(value);
    }

    public List<WhereExpression> getInner() {
        return inner;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public OpType getOpType() {
        return opType;
    }

    public void setOpType(OpType opType) {
        this.opType = opType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
