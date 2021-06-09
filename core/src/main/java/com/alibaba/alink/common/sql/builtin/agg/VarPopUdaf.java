package com.alibaba.alink.common.sql.builtin.agg;

public class VarPopUdaf extends BaseSummaryUdaf {

    public VarPopUdaf() {
        super();
    }

    public VarPopUdaf(boolean dropLast) {
        super(dropLast);
    }

    @Override
    public Number getValue(SummaryData accumulator) {
        return accumulator.getVarPop();
    }
}
