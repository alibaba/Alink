package com.alibaba.alink.common.sql.builtin.agg;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData.FreqData;

public class FreqUdaf extends BaseUdaf<Long, FreqData> {
	private boolean excludeLast = false;

	public FreqUdaf(boolean excludeLast) {
		this.excludeLast = excludeLast;
	}

	public FreqUdaf() {}


	@Override
	public void accumulate(FreqData acc, Object... values) {
		Object value = values[0];
		acc.addData(value);
	}

	@Override
	public void retract(FreqData acc, Object... values) {
		Object value = values[0];
		acc.retractData(value);
	}

	@Override
	public void resetAccumulator(FreqData acc) {
		acc.reset();
	}

	@Override
	public void merge(FreqData acc, Iterable <FreqData> it) {
		for (FreqData data : it) {
			acc.merge(data);
		}
	}

	@Override
	public Long getValue(FreqData accumulator) {
		return accumulator.getValue();
	}

	@Override
	public FreqData createAccumulator() {
		return new FreqData(excludeLast);
	}
}
