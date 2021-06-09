package com.alibaba.alink.common.sql.builtin.agg;

import com.alibaba.alink.common.sql.builtin.agg.DistinctTypeData.IsExistData;

public class IsExistUdaf extends BaseUdaf<Boolean, IsExistData> {

	@Override
	public void accumulate(IsExistData acc, Object... values) {
		Object value = values[0];
		acc.addData(value);
	}

	@Override
	public void retract(IsExistData acc, Object... values) {
		Object value = values[0];
		acc.retractData(value);
	}

	@Override
	public void resetAccumulator(IsExistData acc) {
		acc.reset();
	}

	@Override
	public void merge(IsExistData acc, Iterable <IsExistData> it) {
		for (IsExistData data : it) {
			acc.merge(data);
		}
	}

	@Override
	public Boolean getValue(IsExistData accumulator) {
		return accumulator.getValue();
	}

	@Override
	public IsExistData createAccumulator() {
		return new IsExistData();
	}
}
