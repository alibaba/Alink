package com.alibaba.alink.common.sql.builtin.agg;


public class AvgUdaf extends BaseSummaryUdaf {

    public AvgUdaf(){
		this(false);
    }

    public AvgUdaf(boolean dropLast) {
        super(dropLast, false);
    }
    @Override
    public Number getValue(SummaryData accumulator) {
        return accumulator.getAvg();
    }
}
