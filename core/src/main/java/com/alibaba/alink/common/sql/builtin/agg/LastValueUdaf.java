package com.alibaba.alink.common.sql.builtin.agg;


import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LastValueData;

public class LastValueUdaf extends BaseUdaf<Object, LastValueData> {

	private double timeInterval = -1;
	private final boolean considerNull;

	public LastValueUdaf() {
		this.considerNull = false;
	}

	public LastValueUdaf(boolean considerNull) {
		this.considerNull = considerNull;
	}


	@Override
	public Object getValue(LastValueData accumulator) {
		if (this.considerNull) {
			return accumulator.getDataConsideringNull();
		}
		return accumulator.getData();
	}

	@Override
	public LastValueData createAccumulator() {
		return new LastValueData(considerNull);
	}


	//valueCol, [k, ] timeCol, timeInterval
	@Override
	public void accumulate(LastValueData acc, Object... values) {
		Object value = values[0];
		Object time;
		int k = 0;
		if (values.length == 4) {
			k = (int) values[1];
			time = values[2];
			timeInterval = acc.parseData(values[3], timeInterval);
		} else {
			time = values[1];
			timeInterval = acc.parseData(values[2], timeInterval);
		}

		acc.addData(value, time, k, timeInterval);
	}

	@Override
	public void retract(LastValueData acc, Object... values) {}

	@Override
	public void resetAccumulator(LastValueData acc) {
		acc.reset();
	}

	@Override
	public void merge(LastValueData acc, Iterable <LastValueData> it) {
		LastValueTypeData.merge(acc, it);
	}
}
