package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

/**
 * @author lqb
 * @date 2018/10/19
 */
public class AdaptiveCounter extends LogLogCounter {
	private static final long serialVersionUID = 6625224984940636513L;
	WebTrafficIndexParams.Index counter = WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_ADAPTIVE;

	public AdaptiveCounter(int bit) {
		this(bit, 5);
	}

	public AdaptiveCounter(int bit, int bucketLength) {
		super(bit, bucketLength);
	}

	public AdaptiveCounter(AdaptiveCounter counter) {
		super(counter);
	}

	@Override
	public long count() {
		int c = 0;
		double sum = 0;
		for (int i = 0; i < this.length; i++) {
			int b = getBucketValue(i);
			sum += getBucketValue(i);
			if (b == 0) {
				c++;
			}
		}
		double rate = (double) c / length;
		if (rate >= CounterUtils.EMPTYBUCKETRATE) {
			return CounterUtils.linearCounting(c, length);
		} else {
			double mean = sum / (double) length;
			return (long) (coefficient * Math.pow(2, mean));
		}
	}

	@Override
	public AbstractCounter merge(AbstractCounter counter) {
		if (counter == null) {
			return this;
		}

		AdaptiveCounter adaptiveCounter = (AdaptiveCounter) counter;
		if (adaptiveCounter.counter != this.counter) {
			throw new RuntimeException("Can't merge different counters");
		}
		if (this.bit != adaptiveCounter.bit || this.length != adaptiveCounter.length || this.coefficient
			!= adaptiveCounter.coefficient || this.bucketLength != adaptiveCounter.bucketLength) {
			throw new RuntimeException("Can't merge counters of different sizes");
		}

		AdaptiveCounter newCounter = new AdaptiveCounter(this);

		for (int i = 0; i < length; i++) {
			int val1 = newCounter.getBucketValue(i);
			int val2 = adaptiveCounter.getBucketValue(i);
			if (val1 < val2) {
				newCounter.setBucketValue(i, val2);
			}
		}

		return newCounter;
	}
}
