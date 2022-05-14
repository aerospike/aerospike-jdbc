package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import static com.aerospike.jdbc.predicate.OperatorBinary.*;
import static com.aerospike.jdbc.predicate.OperatorUnary.NOT;

public interface Operator {

    Exp exp(Exp left, Exp right);

    static Operator parsed(SqlOperator op) {
        if (op.kind == SqlKind.EQUALS) return EQ;
        if (op.kind == SqlKind.LESS_THAN) return LT;
        if (op.kind == SqlKind.LESS_THAN_OR_EQUAL) return LE;
        if (op.kind == SqlKind.GREATER_THAN) return GT;
        if (op.kind == SqlKind.GREATER_THAN_OR_EQUAL) return GE;

        if (op.kind == SqlKind.OR) return OR;
        if (op.kind == SqlKind.AND) return AND;

        if (op.kind == SqlKind.NOT) return NOT;

        throw new UnsupportedOperationException("Unsupported operator type");
    }

    static boolean isBoolean(Operator operator) {
        return operator == OR || operator == AND;
    }
}