package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

/**
 * @author lqb
 * @date 2018/10/19
 */
public class LinearCounter extends AbstractCounter {
	private static final long serialVersionUID = -1857434696731368236L;
	//size of the map in bits
	private int length;

	//bitmap
	private byte[] bitMap;

	private WebTrafficIndexParams.Index counter = WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_LINEAR;

	public LinearCounter(int bit) {
		int size = 1 << bit;
		this.length = 8 * size;
		this.bitMap = new byte[size];
	}

	public LinearCounter(LinearCounter counter) {
		this.length = counter.length;
		this.bitMap = counter.bitMap.clone();
	}

	@Override
	public void visit(Object obj) {
		//为了保证hash值是正值
		long hash = CounterUtils.hash128(obj);
		int bit = (int) ((hash & 0xFFFFFFFFL) % (long) length);
		int key = bit >> 3;
		int value = bit & 0x07;
		byte b = bitMap[key];
		bitMap[key] = (byte) (b | (1 << value));
	}

	@Override
	public long count() {
		int c = 0;
		for (byte b : bitMap) {
			c += Integer.bitCount(b & 0xFF);
		}
		int count = length - c;
		if (count == 0) {
			throw new RuntimeException("Unsuitable parameter size, please choose a larger size");
		}
		return CounterUtils.linearCounting(count, length);
	}

	@Override
	public AbstractCounter merge(AbstractCounter counter) {
		if (counter == null) {
			return this;
		}

		LinearCounter linearCounter = (LinearCounter) counter;
		if (linearCounter.counter != this.counter) {
			throw new RuntimeException("Can't merge different counters");
		}
		if (this.length != linearCounter.length) {
			throw new RuntimeException("Can't merge counters of different sizes");
		}

		LinearCounter newCounter = new LinearCounter(this);

		for (int i = 0; i < bitMap.length; i++) {
			newCounter.bitMap[i] |= linearCounter.bitMap[i];
		}

		return newCounter;
	}
}
