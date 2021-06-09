package com.alibaba.alink.common.sql.builtin.agg;


import com.alibaba.alink.common.sql.builtin.agg.LastValueTypeData.LagData;


/**
 * return the last k value.
 */
public class LagUdaf extends BaseUdaf<Object, LagData> {
	private final boolean considerNull;

	public LagUdaf() {
		this.considerNull = false;
	}

	public LagUdaf(boolean considerNull) {
		this.considerNull = considerNull;
	}

	@Override
	public Object getValue(LagData accumulator) {
		if (this.considerNull) {
			return accumulator.getLagDataConsiderNull();
		}
		return accumulator.getLagData();
	}

	@Override
	public LagData createAccumulator() {
		return new LagData(considerNull);
	}

	public void accumulate(LagData acc, Object... values) {
		Object value = values[0];
		int k = 0;
		if (values.length == 1) {
			acc.addLagData(value, k);
		}
		if (values.length == 2) {
			k = (int) values[1];
			acc.addLagData(value, k);
		}
		if (values.length == 3) {
			k = (int) values[1];
			acc.addLagData(value, k, values[2]);
		}
	}

	public void retract(LagData acc, Object... values) {}

	public void resetAccumulator(LagData acc) {
		acc.reset();
	}

	public void merge(LagData acc, Iterable <LagData> it) {
		LastValueTypeData.merge(acc, it);
	}

}
