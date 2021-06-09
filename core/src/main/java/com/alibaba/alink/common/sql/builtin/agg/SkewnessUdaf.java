package com.alibaba.alink.common.sql.builtin.agg;

public class SkewnessUdaf extends BaseSummaryUdaf {

	public SkewnessUdaf() {
		super();
	}

	public SkewnessUdaf(boolean dropLast) {
		super(dropLast);
	}

	@Override
	public Number getValue(SummaryData accumulator) {
		return accumulator.getSkewness();
	}

}