package com.alibaba.alink.pipeline.tuning;

class ValueDistLong extends ValueDist <Long> {

	private static final long serialVersionUID = -1249613347196618632L;
	private long start;
	private long n;

	public ValueDistLong(long start, long end) {
		this.start = Math.min(start, end);
		this.n = Math.abs(end - start) + 1;
	}

	@Override
	public Long get(double p) {
		return start + Math.min(n - 1, (int) (Math.floor(n * p)));
	}
}
