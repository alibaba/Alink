package com.alibaba.alink.common.sql.builtin.agg;


public class AvgUdaf extends BaseSummaryUdaf {

    public AvgUdaf() {
        super();
    }

    public AvgUdaf(boolean dropLast) {
        super(dropLast);
    }
    @Override
    public Number getValue(SummaryData accumulator) {
        return accumulator.getAvg();
    }
}
