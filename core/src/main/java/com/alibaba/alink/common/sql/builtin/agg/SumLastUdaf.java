package com.alibaba.alink.common.sql.builtin.agg;

import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LastValueData;
import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.SumLastData;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Sum of last k data (include the recent received data). If there are less than k data in the
 * current window, return the sum of all the data in the window.
 */
public class SumLastUdaf extends BaseUdaf<Object, SumLastData> {
	private double timeInterval = -1;

	public SumLastUdaf() {}

	public SumLastUdaf(int k, double timeInterval) {
		if (k <= 0) {
			throw new RuntimeException("k must be set larger than 0.");
		}
		this.timeInterval = timeInterval * 1000;
	}

	@Override
	public Object getValue(SumLastData accumulator) {
		return accumulator.getData();
	}

	@Override
	public SumLastData createAccumulator() {
		return new SumLastData();
	}

	public void accumulate(SumLastData acc, Object... values) {
		Object value = values[0];
		int k = 1;
		if (value == null) {
			value = 0;
		}
		Object time;
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

	public void retract(SumLastData acc, Object... values) {}

	public void resetAccumulator(SumLastData acc) {
		acc.reset();
	}

	public void merge(SumLastData acc, Iterable <SumLastData> it) {
		Iterable <LastValueData> its =
			StreamSupport.stream(it.spliterator(), false).map(x -> (LastValueData) x)
				.collect(Collectors.toList());
		LastValueTypeData.merge(acc, its);
	}
}
