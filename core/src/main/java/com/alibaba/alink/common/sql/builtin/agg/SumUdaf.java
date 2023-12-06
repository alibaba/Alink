package com.alibaba.alink.common.sql.builtin.agg;


public class SumUdaf extends BaseSummaryUdaf {

    public SumUdaf() {
        this(false);
    }

    public SumUdaf(boolean dropLast) {
        super(dropLast, false);
    }

    @Override
    public Number getValue(SummaryData accumulator) {
        return accumulator.getSum();
    }
}
