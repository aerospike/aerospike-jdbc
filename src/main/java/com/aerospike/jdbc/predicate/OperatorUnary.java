package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;

import java.util.function.Function;

public enum OperatorUnary implements Operator {

    NOT(Exp::not);

    private final Function<Exp, Exp> func;

    OperatorUnary(Function<Exp, Exp> expFunc) {
        this.func = expFunc;
    }

    public Exp exp(Exp left, Exp right) {
        return func.apply(left);
    }
}
