package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

/**
 * @author lqb
 * @date 2018/10/19
 */
public class HyperLogLogCounter extends LogLogCounter {
	private static final long serialVersionUID = -5405512498936063926L;
	private WebTrafficIndexParams.Index counter = WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_HYPERLOGLOG;

	public HyperLogLogCounter() {}

	public HyperLogLogCounter(int bit) {
		this(bit, 5);
	}

	public HyperLogLogCounter(int bit, int bucketLength) {
		super(bit, bucketLength);
		this.coefficient = getCoefficient(bit, length);
	}

	public HyperLogLogCounter(HyperLogLogCounter counter) {
		super(counter);
	}

	@Override
	public long count() {
		double sum = 0.0;
		int c = 0;
		for (int i = 0; i < length; i++) {
			int b = getBucketValue(i);
			sum += Math.pow(2, -1 * b);
			if (b == 0) {
				c++;
			}
		}

		double estimate = coefficient * (1 / sum);

		if (estimate <= (5.0 / 2.0) * length) {
			return CounterUtils.linearCounting(c, length);
		} else if (estimate <= (1.0 / 30.0) * CounterUtils.POW_2_32) {
			return Math.round(estimate);
		} else {
			return Math.round((CounterUtils.NEGATIVE_POW_2_32 * Math.log(1.0 - (estimate / CounterUtils.POW_2_32))));
		}
	}

	@Override
	public AbstractCounter merge(AbstractCounter counter) {
		if (counter == null) {
			return this;
		}

		HyperLogLogCounter hyperLogLogCounter = (HyperLogLogCounter) counter;
		if (hyperLogLogCounter.counter != this.counter) {
			throw new RuntimeException("Can't merge different counters");
		}
		if (this.bit != hyperLogLogCounter.bit || this.length != hyperLogLogCounter.length || this.coefficient
			!= hyperLogLogCounter.coefficient || this.bucketLength != hyperLogLogCounter.bucketLength) {
			throw new RuntimeException("Can't merge counters of different sizes");
		}

		HyperLogLogCounter newCounter = new HyperLogLogCounter(this);

		for (int i = 0; i < length; i++) {
			int val1 = newCounter.getBucketValue(i);
			int val2 = hyperLogLogCounter.getBucketValue(i);
			if (val1 < val2) {
				newCounter.setBucketValue(i, val2);
			}
		}

		return newCounter;
	}

	protected double getCoefficient(int m, int length) {
		switch (m) {
			case 4:
				return 0.673 * length * length;
			case 5:
				return 0.697 * length * length;
			case 6:
				return 0.709 * length * length;
			default:
				return (0.7213 / (1 + 1.079 / length)) * length * length;
		}
	}
}
