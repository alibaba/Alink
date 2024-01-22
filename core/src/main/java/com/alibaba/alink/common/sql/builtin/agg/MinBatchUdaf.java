package com.alibaba.alink.common.sql.builtin.agg;

import com.alibaba.alink.common.sql.builtin.agg.MinBatchUdaf.MinMaxBatchData;

public class MinBatchUdaf extends BaseUdaf <Object, MinMaxBatchData> {

	public MinBatchUdaf() {}

	@Override
	public void accumulate(MinMaxBatchData max, Object... values) {
		Object value = values[0];
		if (value == null) {
			return;
		}
		if (max.minMax == null) {
			max.minMax = value;
			return;
		}
		if (((Comparable) max.minMax).compareTo(value) > 0) {
			max.minMax = value;
		}

	}

	@Override
	public void resetAccumulator(MinMaxBatchData minMaxData) {
		minMaxData.minMax = null;
	}

	@Override
	public void merge(MinMaxBatchData minMaxData, Iterable <MinMaxBatchData> it) {
		for (MinMaxBatchData data : it) {
			if (minMaxData.minMax == null) {
				minMaxData.minMax = data.minMax;
			} else if (data.minMax == null) {

			} else {
				if (((Comparable) minMaxData.minMax).compareTo(data.minMax) > 0) {
					minMaxData.minMax = data.minMax;
				}
			}
		}
	}

	@Override
	public Object getValue(MinMaxBatchData accumulator) {
		return accumulator.minMax;
	}

	@Override
	public MinMaxBatchData createAccumulator() {
		return new MinBatchUdaf.MinMaxBatchData();
	}

	public static class MinMaxBatchData {
		public Object minMax;

		public MinMaxBatchData() {

		}
	}
}
