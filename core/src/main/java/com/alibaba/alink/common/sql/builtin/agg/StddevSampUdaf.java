package com.alibaba.alink.common.sql.builtin.agg;

public class StddevSampUdaf extends BaseSummaryUdaf {

    public StddevSampUdaf() {
        super();
    }

    public StddevSampUdaf(boolean dropLast) {
        super(dropLast);
    }

    @Override
    public Number getValue(SummaryData accumulator) {
        return accumulator.getStdSamp();
    }
}
