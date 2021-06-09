package com.alibaba.alink.common.sql.builtin.agg;

import org.apache.flink.table.functions.AggregateFunction;

public abstract class BaseUdaf<T, ACC> extends AggregateFunction<T, ACC> {

	public abstract void accumulate(ACC acc, Object... values);

	public abstract void retract(ACC acc, Object... values);

	public abstract void resetAccumulator(ACC acc);

	public abstract void merge(ACC acc, Iterable<ACC> it);

	ACC acc;
	public void accumulateBatch(Object... values) {
		if (acc == null) {
			acc = createAccumulator();
		}
		accumulate(acc, values);
	}

	public T getValueBatch() {
		return getValue(acc);
	}

}
