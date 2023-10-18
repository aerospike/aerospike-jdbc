package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;
import com.google.common.base.Preconditions;

import java.util.function.BinaryOperator;

public enum OperatorBinary implements Operator {

    EQ(Exp::eq),
    LT(Exp::lt),
    LE(Exp::le),
    GT(Exp::gt),
    GE(Exp::ge),
    NE((exp1, exp2) -> Exp.not(Exp.eq(exp1, exp2))),

    OR(Exp::or),
    AND(Exp::and);

    private final BinaryOperator<Exp> func;

    OperatorBinary(BinaryOperator<Exp> expFunc) {
        this.func = expFunc;
    }

    @Override
    public Exp exp(Exp... expressions) {
        Preconditions.checkArgument(expressions.length == 2);
        return func.apply(expressions[0], expressions[1]);
    }
}
