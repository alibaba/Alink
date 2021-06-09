package com.alibaba.alink.common.sql.builtin.agg;


public class SumUdaf extends BaseSummaryUdaf {

    public SumUdaf() {
        super();
    }

    public SumUdaf(boolean dropLast) {
        super(dropLast);
    }

    @Override
    public Number getValue(SummaryData accumulator) {
        return accumulator.getSum();
    }
}
