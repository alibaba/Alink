package com.alibaba.alink.common.sql.builtin.agg;

public class SummaryUdaf extends BaseSummaryUdaf {

	public SummaryUdaf() {
		this(false);
	}

	public SummaryUdaf(boolean dropLast) {
		super(dropLast);

	}

	@Override
	public SummaryData getValue(SummaryData accumulator) {
		return accumulator;
	}

	@Override
	public void accumulate(SummaryData acc, Object... values) {
		Object data = values[0];
		if (data == "*") {
			acc.addData(1);
			return;
		}
		if (data == null) {
			return;
		}
		acc.addData(1);

	}

	@Override
	public void retract(SummaryData acc, Object... values) {
		Object data = values[0];
		if (data == "*") {
			acc.retractData(1);
			return;
		}
		if (data == null) {
			return;
		}
		acc.retractData(1);
	}

}
