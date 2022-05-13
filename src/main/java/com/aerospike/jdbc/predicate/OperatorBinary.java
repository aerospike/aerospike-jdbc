package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;

import java.util.function.BiFunction;

public enum OperatorBinary implements Operator {

    EQ(Exp::eq),
    LT(Exp::lt),
    LE(Exp::le),
    GT(Exp::gt),
    GE(Exp::ge),

    OR(Exp::or),
    AND(Exp::and);

    private final BiFunction<Exp, Exp, Exp> func;

    OperatorBinary(BiFunction<Exp, Exp, Exp> expFunc) {
        this.func = expFunc;
    }

    public Exp exp(Exp left, Exp right) {
        return func.apply(left, right);
    }
}
