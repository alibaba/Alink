package com.alibaba.alink.common.sql.builtin.agg;

public class VarSampUdaf extends BaseSummaryUdaf {

    public VarSampUdaf() {
        super();
    }

    public VarSampUdaf(boolean dropLast) {
        super(dropLast);
    }

    @Override
    public Number getValue(SummaryData accumulator) {
        return accumulator.getVarSamp();
    }
}
