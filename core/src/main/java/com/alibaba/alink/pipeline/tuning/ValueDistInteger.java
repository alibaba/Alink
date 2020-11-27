package com.alibaba.alink.pipeline.tuning;

class ValueDistInteger extends ValueDist <Integer> {

	private static final long serialVersionUID = -9010272438179761545L;
	private int start;
	private int n;

	public ValueDistInteger(int start, int end) {
		this.start = Math.min(start, end);
		this.n = Math.abs(end - start) + 1;
	}

	@Override
	public Integer get(double p) {
		return start + Math.min(n - 1, (int) (Math.floor(n * p)));
	}
}
