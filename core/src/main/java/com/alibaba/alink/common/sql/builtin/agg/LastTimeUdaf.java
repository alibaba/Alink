package com.alibaba.alink.common.sql.builtin.agg;


import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LastTimeData;
import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LastValueData;

import java.sql.Timestamp;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class LastTimeUdaf extends BaseUdaf<Object, LastTimeData> {

	private double timeInterval = -1;

	public LastTimeUdaf() {}

	public LastTimeUdaf(int k, double timeInterval) {
		if (k <= 0) {
			throw new AkIllegalOperatorParameterException("k must be set larger than 0.");
		}
		this.timeInterval = timeInterval * 1000;
	}

	@Override
	public Object getValue(LastTimeData accumulator) {
		return accumulator.getData();
	}

	@Override
	public LastTimeData createAccumulator() {
		return new LastTimeData();
	}


	@Override
	public void accumulate(LastTimeData acc, Object... values) {
		int k = 0;
		Object time;
		if (values.length == 4) {
			k = (int) values[1];
			time = values[2];
			timeInterval = acc.parseData(values[3], timeInterval);
		} else {
			time = values[1];
			timeInterval = acc.parseData(values[2], timeInterval);
		}

		acc.addData(values[0], time, k, timeInterval);
	}

	@Override
	public void retract(LastTimeData acc, Object... values) {}

	@Override
	public void resetAccumulator(LastTimeData acc) {
		acc.reset();
	}

	@Override
	public void merge(LastTimeData acc, Iterable <LastTimeData> it) {
		Iterable<LastValueData> its =
			StreamSupport.stream(it.spliterator(), false).map(x->(LastValueData) x)
				.collect(Collectors.toList());
		LastValueTypeData.merge(acc, its);
	}
}