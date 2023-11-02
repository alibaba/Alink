package com.alibaba.alink.operator.common.dataproc.counter;

import com.alibaba.alink.params.dataproc.WebTrafficIndexParams;

/**
 * @author lqb
 * @date 2018/10/19
 */
public class LogLogCounter extends AbstractCounter {
	/**
	 * Gamma function computed using Mathematica
	 * AccountingForm[
	 * N[With[{m = 2^Range[0, 31]},
	 * 渐进无偏估计量
	 * m (Gamma[-1/m]*(1 - 2^(1/m))/Log[2])^-m], 14]]
	 */
	protected static final double[] M_ALPHA = {
		0,
		0.44567926005415,
		1.2480639342271,
		2.8391255240079,
		6.0165231584809,
		12.369319965552,
		25.073991603111,
		50.482891762408,
		101.30047482584,
		202.93553338100,
		406.20559696699,
		812.74569744189,
		1625.8258850594,
		3251.9862536323,
		6504.3069874480,
		13008.948453415,
		26018.231384516,
		52036.797246302,
		104073.92896967,
		208148.19241629,
		416296.71930949,
		832593.77309585,
		1665187.8806686,
		3330376.0958140,
		6660752.5261049,
		13321505.386687,
		26643011.107850,
		53286022.550177,
		106572045.43483,
		213144091.20414,
		426288182.74275,
		852576365.81999
	};
	private static final long serialVersionUID = -6036647762939706318L;

	double coefficient;
	int length;
	int bit;
	byte[] bitMap;
	int bucketLength;

	private WebTrafficIndexParams.Index counter = WebTrafficIndexParams.Index.CARDINALITY_ESTIMATE_LOGLOG;

	public LogLogCounter() {}

	public LogLogCounter(int bit) {
		this(bit, 5);
	}

	public LogLogCounter(int bit, int bucketLength) {
		if (bit >= (M_ALPHA.length - 1)) {
			throw new IllegalArgumentException(String.format("Max k (%d) exceeded: k=%d", M_ALPHA.length - 1, bit));
		}
		this.bit = bit;
		this.length = 1 << bit;
		this.bucketLength = bucketLength;
		bitMap = new byte[getBitMapLength(length)];
		this.coefficient = M_ALPHA[bit];
	}

	public LogLogCounter(LogLogCounter counter) {
		this.coefficient = counter.coefficient;
		this.length = counter.length;
		this.bit = counter.bit;
		this.bucketLength = counter.bucketLength;
		if (counter.bitMap != null) {
			this.bitMap = counter.bitMap.clone();
		}
	}

	protected int getBitMapLength(int length) {
		int size = length * bucketLength;
		if (size % CounterUtils.BYTE_LENGTH == 0) {
			return size / CounterUtils.BYTE_LENGTH;
		} else {
			return size / CounterUtils.BYTE_LENGTH + 1;
		}
	}

	protected void update(int bucket, int value) {
		int origin = getBucketValue(bucket);
		if (origin < value) {
			setBucketValue(bucket, value);
		}
	}

	protected byte getBucketValue(int bucket) {
		int byteStartIndex = (bucket * this.bucketLength) / CounterUtils.BYTE_LENGTH;
		int byteStartValue = (bucket * this.bucketLength) % CounterUtils.BYTE_LENGTH;
		int byteEndIndex = (bucket * this.bucketLength + this.bucketLength) / CounterUtils.BYTE_LENGTH;
		int byteEndValue = (bucket * this.bucketLength + this.bucketLength) % CounterUtils.BYTE_LENGTH;
		int start = this.bitMap[byteStartIndex] & 0xff;
		byte value = (byte) ((((byte) (start << byteStartValue)) & 0xff) >>> (CounterUtils.BYTE_LENGTH
			- this.bucketLength));
		if (byteEndIndex != byteStartIndex && byteEndValue != 0) {
			int end = this.bitMap[byteEndIndex] & 0xff;
			value = (byte) ((value & 0xff) | (end >>> (CounterUtils.BYTE_LENGTH - byteEndValue)));
		}
		return value;
	}

	protected void setBucketValue(int bucket, int value) {
		int byteStartIndex = (bucket * this.bucketLength) / CounterUtils.BYTE_LENGTH;
		int byteStartValue = (bucket * this.bucketLength) % CounterUtils.BYTE_LENGTH;
		int byteEndIndex = (bucket * this.bucketLength + this.bucketLength) / CounterUtils.BYTE_LENGTH;
		int byteEndValue = (bucket * this.bucketLength + this.bucketLength) % CounterUtils.BYTE_LENGTH;
		if (byteEndIndex == byteStartIndex) {
			int origin = this.bitMap[byteEndIndex] & 0xff;
			this.bitMap[byteEndIndex] = (byte) ((origin & ((0xff >>> byteStartValue) ^ 0xfff)) | (origin & (
				(0xff << (CounterUtils.BYTE_LENGTH - byteEndValue)) ^ 0xff)) | (value << (CounterUtils.BYTE_LENGTH
				- byteEndValue)));
		} else {
			int origin = this.bitMap[byteStartIndex] & 0xff;
			this.bitMap[byteStartIndex] = (byte) (((0xff << (CounterUtils.BYTE_LENGTH - byteStartValue)) & origin) | (
				value >>> (this.bucketLength + byteStartValue - CounterUtils.BYTE_LENGTH)));
			if (byteEndValue != 0) {
				origin = this.bitMap[byteEndIndex] & 0xff;
				this.bitMap[byteEndIndex] = (byte) (((0xff >>> byteEndValue) & origin) | (value << (
					CounterUtils.BYTE_LENGTH - byteEndValue)));
			}
		}
	}

	@Override
	public void visit(Object obj) {
		int hashValue = CounterUtils.hash128(obj).intValue();
		int j = hashValue >>> (Integer.SIZE - this.bit);
		int r = Integer.numberOfLeadingZeros((hashValue << this.bit) | (1 << (this.bit - 1))) + 1;
		update(j, r);
	}

	@Override
	public long count() {
		double sum = 0;
		for (int i = 0; i < this.length; i++) {
			sum += getBucketValue(i);
		}
		double mean = sum / (double) length;
		return (long) (coefficient * Math.pow(2, mean));
	}

	@Override
	public AbstractCounter merge(AbstractCounter counter) {
		if (counter == null) {
			return this;
		}

		LogLogCounter logLogCounter = (LogLogCounter) counter;
		if (logLogCounter.counter != this.counter) {
			throw new RuntimeException("Can't merge different counters");
		}
		if (this.bit != logLogCounter.bit || this.length != logLogCounter.length || this.coefficient
			!= logLogCounter.coefficient || this.bucketLength != logLogCounter.bucketLength) {
			throw new RuntimeException("Can't merge counters of different sizes");
		}

		LogLogCounter newCounter = new LogLogCounter(this);

		for (int i = 0; i < length; i++) {
			int val1 = newCounter.getBucketValue(i);
			int val2 = logLogCounter.getBucketValue(i);
			if (val1 < val2) {
				newCounter.setBucketValue(i, val2);
			}
		}

		return newCounter;
	}
}
