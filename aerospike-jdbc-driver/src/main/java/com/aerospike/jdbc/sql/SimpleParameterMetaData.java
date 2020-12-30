package com.aerospike.jdbc.sql;

import com.aerospike.jdbc.model.DataColumn;
import com.aerospike.jdbc.util.SqlLiterals;

import java.sql.ParameterMetaData;
import java.util.List;
import java.util.Optional;

public class SimpleParameterMetaData implements ParameterMetaData, SimpleWrapper {

    private final List<DataColumn> columns;

    public SimpleParameterMetaData(List<DataColumn> columns) {
        this.columns = columns;
    }

    @Override
    public int getParameterCount() {
        return columns.size();
    }

    @Override
    public int isNullable(int param) {
        return ParameterMetaData.parameterNullable;
    }

    @Override
    public boolean isSigned(int param) {
        return false;
    }

    @Override
    public int getPrecision(int param) {
        return 0;
    }

    @Override
    public int getScale(int param) {
        return 0;
    }

    @Override
    public int getParameterType(int param) {
        return columns.get(param - 1).getType();
    }

    @Override
    public String getParameterTypeName(int param) {
        return SqlLiterals.sqlTypeNames.get(getParameterType(param));
    }

    @Override
    public String getParameterClassName(int param) {
        return Optional.ofNullable(SqlLiterals.sqlToJavaTypes.get(getParameterType(param)))
                .map(Class::getName).orElse(null);
    }

    @Override
    public int getParameterMode(int param) {
        return ParameterMetaData.parameterModeIn;
    }
}
