package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

/**
 * @author lqb
 * @date 2018/10/19
 */
public class StochasticCounter extends AbstractCounter {
	private static final long serialVersionUID = 8216495099178298695L;
	private int[] bitMap;
	private int length;
	private int bit;
	private WebTrafficIndexParams.Index counter = WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_STOCHASTIC;

	public StochasticCounter(int bit) {
		this.bit = bit;
		this.length = (int) Math.pow(2, bit);
		this.bitMap = new int[this.length];
	}

	public StochasticCounter(StochasticCounter counter) {
		this.bit = counter.bit;
		this.length = counter.length;
		this.bitMap = counter.bitMap.clone();
	}

	@Override
	public void visit(Object obj) {
		int hashValue = CounterUtils.hash128(obj).intValue();
		int index = hashValue >>> (Integer.SIZE - this.bit);
		int position = hashValue & ((1 << (Integer.SIZE - this.bit)) - 1);
		bitMap[index] = bitMap[index] | (1 << getLeastSignificantZero(position));
	}

	@Override
	public long count() {
		long sum = 0L;
		for (int i = 0; i < length; i++) {
			sum += getLeastSignificantZero(bitMap[i]);
		}
		double mean = 1.0 * sum / length;
		double res = length * Math.pow(2, mean) / 0.77351;
		return (long) res;
	}

	@Override
	public AbstractCounter merge(AbstractCounter counter) {
		if (counter == null) {
			return this;
		}

		StochasticCounter stochasticCounter = (StochasticCounter) counter;

		if (stochasticCounter.counter != this.counter) {
			throw new RuntimeException("Can't merge different counters");
		}
		if (this.bit != stochasticCounter.bit || this.length != stochasticCounter.length) {
			throw new RuntimeException("Can't merge counters of different sizes");
		}

		StochasticCounter newCounter = new StochasticCounter(this);
		for (int i = 0; i < this.length; i++) {
			newCounter.bitMap[i] |= stochasticCounter.bitMap[i];
		}
		return newCounter;
	}

	private int getLeastSignificantZero(int value) {
		int v = value ^ (value + 1);
		return Integer.SIZE - Integer.numberOfLeadingZeros(v) - 1;
	}
}
