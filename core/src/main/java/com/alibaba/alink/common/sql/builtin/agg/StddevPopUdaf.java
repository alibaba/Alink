package com.alibaba.alink.common.sql.builtin.agg;

public class StddevPopUdaf extends BaseSummaryUdaf {

    public StddevPopUdaf() {
        super();
    }

    public StddevPopUdaf(boolean dropLast) {
        super(dropLast);
    }

    @Override
    public Number getValue(SummaryData accumulator) {
        return accumulator.getStdPop();
    }
}
