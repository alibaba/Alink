package com.alibaba.alink.common.sql.builtin.agg;

import org.apache.flink.table.functions.AggregateFunction;

public abstract class BaseUdaf<T, ACC> extends AggregateFunction <T, ACC> {

	public void accumulate(ACC acc, Object... values) {}

	public void retract(ACC acc, Object... values) {}

	public void resetAccumulator(ACC acc) {}

	public void merge(ACC acc, Iterable <ACC> it) {}

	ACC acc;

	public void accumulateBatch(Object... values) {
		createAccumulatorAndSet();
		accumulate(acc, values);
	}

	public T getValueBatch() {
		return getValue(acc);
	}

	private void createAccumulatorAndSet() {
		if (acc == null) {
			acc = createAccumulator();
		}
	}

}
