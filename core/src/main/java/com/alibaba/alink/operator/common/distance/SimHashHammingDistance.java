package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.SimilarityUtil;

import java.math.BigInteger;

/**
 * Calculate the simHashHamming distance between two str.
 */
public class SimHashHammingDistance implements CategoricalDistance, FastCategoricalDistance <BigInteger> {
	/**
	 * Bit length for simhash, only support 64bits and 128bits.
	 */
	private static final long serialVersionUID = 5022730767279235084L;
	private int bitLength;

	public SimHashHammingDistance(int bitLength) {
		if (64 != bitLength && 128 != bitLength) {
			throw new RuntimeException("bitNum should be 64 or 128.");
		}
		this.bitLength = bitLength;
	}

	@Override
	public int calc(String str1, String str2) {
		String[] s1 = SimilarityUtil.splitStringToWords(str1);
		String[] s2 = SimilarityUtil.splitStringToWords(str2);
		return calc(s1, s2);
	}

	@Override
	public int calc(String[] str1, String[] str2) {
		BigInteger b1 = simHash(str1);
		BigInteger b2 = simHash(str2);
		return hammingDistance(b1, b2);
	}

	public int hammingDistance(BigInteger left, BigInteger right) {
		return left.xor(right).bitCount();
	}

	/**
	 * Hash the vector to BIT_LENGTH size and add the weights.
	 */
	public <T> BigInteger simHash(T str) {
		String[] words;
		if (str instanceof String) {
			words = SimilarityUtil.splitStringToWords((String) str);
		} else {
			words = (String[]) str;
		}
		if (null == words || words.length == 0) {
			return BigInteger.ZERO;
		}
		// Count the word frequency.
		int[] v = new int[this.bitLength];
		for (String temp : words) {
			BigInteger t = this.hash(temp);
			for (int i = 0; i < this.bitLength; i++) {
				BigInteger bitmask = BigInteger.ONE.shiftLeft(i);
				if (t.and(bitmask).signum() != 0) {
					v[i] += 1;
				} else {
					v[i] -= 1;
				}
			}
		}
		BigInteger fingerprint = BigInteger.ZERO;
		for (int i = 0; i < this.bitLength; i++) {
			if (v[i] >= 0) {
				fingerprint = fingerprint.add(BigInteger.ONE.shiftLeft(i));
			}
		}
		return fingerprint;
	}

	private BigInteger hash(String source) {
		if (null == source || source.length() == 0) {
			return BigInteger.ZERO;
		}
		char[] sourceArray = source.toCharArray();
		BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
		BigInteger m = new BigInteger("1000003");
		BigInteger mask = new BigInteger("2").pow(bitLength).subtract(
			BigInteger.ONE);
		for (char item : sourceArray) {
			BigInteger temp = BigInteger.valueOf((long) item);
			x = x.multiply(m).xor(temp).and(mask);
		}
		x = x.xor(new BigInteger(String.valueOf(source.length())));
		if (x.equals(new BigInteger("-1"))) {
			x = new BigInteger("-2");
		}
		return x;
	}

	@Override
	public <M> void updateLabel(Sample sample, M str) {
		sample.setStr(null);
		sample.setLabel(simHash(str));
	}

	@Override
	public double calc(Sample <BigInteger> left, Sample <BigInteger> right, boolean text) {
		return hammingDistance(left.getLabel(), right.getLabel());
	}
}
