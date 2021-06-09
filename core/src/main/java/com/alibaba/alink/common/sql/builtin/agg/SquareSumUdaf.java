package com.alibaba.alink.common.sql.builtin.agg;


public class SquareSumUdaf extends BaseSummaryUdaf {

	public SquareSumUdaf() {
		super();
	}

	public SquareSumUdaf(boolean dropLast) {
		super(dropLast);
	}

	@Override
	public Number getValue(SummaryData accumulator) {
		return accumulator.getSquareSum();
	}
}
