package com.alibaba.alink.common.sql.builtin.agg;



import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData.ModeData;

public class ModeUdaf extends BaseUdaf<Object, ModeData> {

	private boolean excludeLast = false;

	public ModeUdaf() {}

	public ModeUdaf(boolean excludeLast) {
		this.excludeLast = excludeLast;
	}

	@Override
	public Object getValue(ModeData accumulator) {
		return accumulator.getValue();
	}

	@Override
	public ModeData createAccumulator() {
		return new ModeData(excludeLast);
	}

	@Override
	public void accumulate(ModeData acc, Object... values) {
		Object value = values[0];
		acc.addData(value);
	}

	@Override
	public void resetAccumulator(ModeData acc) {
		acc.reset();
	}

	@Override
	public void retract(ModeData acc, Object... values) {
		Object value = values[0];
		acc.retractData(value);
	}

	@Override
	public void merge(ModeData acc, Iterable <ModeData> it) {
		for (ModeData modeData : it) {
			acc.merge(modeData);
		}
	}
}
