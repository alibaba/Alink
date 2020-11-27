package com.alibaba.alink.pipeline.tuning;

class ValueDistArray<V> extends ValueDist <V> {

	private static final long serialVersionUID = 9098772041562392815L;
	private V[] values;
	private int n;

	public ValueDistArray(V[] values) {
		this.values = values;
		this.n = this.values.length;
	}

	@Override
	public V get(double p) {
		return values[Math.min(n - 1, (int) (Math.floor(n * p)))];
	}
}
