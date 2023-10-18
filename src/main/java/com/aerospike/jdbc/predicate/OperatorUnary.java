package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.google.common.base.Preconditions;

import java.util.function.UnaryOperator;

public enum OperatorUnary implements Operator {

    NOT(Exp::not);

    private final UnaryOperator<Exp> func;

    OperatorUnary(UnaryOperator<Exp> expFunc) {
        this.func = expFunc;
    }

    @Override
    public Exp exp(Exp... expressions) {
        Preconditions.checkArgument(expressions.length == 1);
        return func.apply(expressions[0]);
    }
}
