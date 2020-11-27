package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.distance.SimHashHammingDistance;
import com.alibaba.alink.operator.common.similarity.Sample;

import java.math.BigInteger;

/**
 * Calculate the SimHash Hamming Distance.
 * SimHash Hamming: Hash the inputs to BIT_LENGTH size, and calculate the hamming metric.
 */
public class SimHashHammingSimilarity extends Similarity <BigInteger> {
	private static final long serialVersionUID = -9100789012867575015L;
	public int BIT_LENGTH = 64;

	/**
	 * Set the parameter K.
	 */
	public SimHashHammingSimilarity() {
		this.distance = new SimHashHammingDistance(BIT_LENGTH);
	}

	@Override
	public <M> void updateLabel(Sample sample, M str) {
		sample.setStr(null);
		sample.setLabel(((SimHashHammingDistance) distance).simHash(str));
	}

	@Override
	public double calc(Sample <BigInteger> left, Sample <BigInteger> right, boolean text) {
		return 1 - ((SimHashHammingDistance) distance).calc(left, right, text) / (double) BIT_LENGTH;
	}

	/**
	 * Override the calc function of Similarity.
	 */
	@Override
	public double calc(String left, String right) {
		return 1 - distance.calc(left, right) / (double) BIT_LENGTH;
	}

	@Override
	public double calc(String[] left, String[] right) {
		return 1 - distance.calc(left, right) / (double) BIT_LENGTH;
	}

	public double similarity(BigInteger left, BigInteger right) {
		return 1 - ((SimHashHammingDistance) distance).hammingDistance(left, right) / (double) BIT_LENGTH;
	}
}
