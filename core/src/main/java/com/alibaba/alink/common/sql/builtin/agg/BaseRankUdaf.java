package com.alibaba.alink.common.sql.builtin.agg;

import java.sql.Timestamp;

public abstract class BaseRankUdaf extends BaseUdaf<Long, RankData> {

	public BaseRankUdaf() {}

	@Override
	public Long getValue(RankData accumulator) {
		return accumulator.count();
	}

	@Override
	public RankData createAccumulator() {
		return new RankData();
	}


	public void accumulateTemp(RankData acc, Object... values) {
		if (values == null) {
			return;
		}
		Number[] data = new Number[values.length];
		for (int i = 0; i < values.length; i++) {
			if (values[i] instanceof Timestamp) {
				data[i] = ((Timestamp) values[i]).getTime();
			} else {
				data[i] = (Number) values[i];
			}
		}
		acc.addData(data);
	}

	public void accumulate(RankData acc, Object value) {
		accumulate(acc, new Object[]{value});
	}

	public void retract(RankData acc, Object value) {}

	@Override
	public void resetAccumulator(RankData acc) {
		acc.reset();
	}

	@Override
	public void retract(RankData acc, Object... values) {
	}

	@Override
	public void merge(RankData acc, Iterable <RankData> it) {}

}
