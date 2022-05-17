package com.aerospike.jdbc.predicate;

import com.aerospike.client.exp.Exp;

public enum OperatorVarArgs implements Operator {

    IN(Exp::or);

    private final VarArgsFunction<Exp, Exp> func;

    OperatorVarArgs(VarArgsFunction<Exp, Exp> expFunc) {
        this.func = expFunc;
    }

    @Override
    public Exp exp(Exp... expressions) {
        return func.apply(expressions);
    }
}
