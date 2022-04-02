package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.similarity.Sample;

import java.util.HashMap;
import java.util.Map;

/**
 * Calculate the cosine calc.
 * Cosine calc: a measure of calc between two non-zero vectors of an inner product
 * space that measures the cosine of the angle between them.
 */

public class Cosine extends Similarity <Double> {
	/**
	 * The length of the slice window.
	 */
	private static final long serialVersionUID = -8552591335185092365L;
	private int k;

	/**
	 * Set the parameter k.
	 */
	public Cosine(int k) {
		if (k <= 0) {
			throw new RuntimeException("k must be positive!");
		}
		this.k = k;
	}

	public <T> double calCosine(T lsv, T rsv) {
		int lenL, lenR;
		if (lsv instanceof String) {
			lenL = ((String) lsv).length();
			lenR = ((String) rsv).length();
		} else {
			lenL = ((String[]) lsv).length;
			lenR = ((String[]) rsv).length;
		}
		Map <String, int[]> map = new HashMap <>(0);
		for (int i = 0; i < Math.max(lenL - k + 1, 1); i++) {
			String key = window(lsv, i);
			if (map.containsKey(key)) {
				map.get(key)[0] += 1;
			} else {
				int[] tmp = new int[2];
				tmp[0] = 1;
				tmp[1] = 0;
				map.put(key, tmp);
			}
		}
		for (int i = 0; i < Math.max(lenR - k + 1, 1); i++) {
			String key = window(rsv, i);
			if (map.containsKey(key)) {
				map.get(key)[1] += 1;
			} else {
				int[] tmp = new int[2];
				tmp[0] = 0;
				tmp[1] = 1;
				map.put(key, tmp);
			}
		}
		double ret = 0;
		for (Map.Entry <String, int[]> entry : map.entrySet()) {
			ret += (entry.getValue()[0] * entry.getValue()[1]);
		}
		return ret;
	}

	public <T> double calCosine(T lsv) {
		int lenL;
		if (lsv instanceof String) {
			lenL = ((String) lsv).length();
		} else {
			lenL = ((String[]) lsv).length;
		}
		Map <String, Integer> map = new HashMap <>(0);
		for (int i = 0; i < Math.max(lenL - k + 1, 1); i++) {
			String key = window(lsv, i);
			if (map.containsKey(key)) {
				map.put(key, map.get(key) + 1);
			} else {
				map.put(key, 1);
			}
		}
		double ret = 0;
		for (Map.Entry <String, Integer> entry : map.entrySet()) {
			ret += entry.getValue() * entry.getValue();
		}
		return ret;
	}

	private <T> String window(T s, int start) {
		if (s instanceof String) {
			String str = (String) s;
			return str.substring(start, Math.min(start + k, str.length()));
		} else {
			StringBuilder builder = new StringBuilder();
			String[] str = (String[]) s;
			for (int j = start; j < Math.min(start + k, str.length); j++) {
				builder.append(str[j]);
			}
			return builder.toString();
		}
	}

	@Override
	public <M> void updateLabel(Sample sample, M str) {
		sample.setLabel(calCosine(str));
	}

	@Override
	public double calc(Sample <Double> left, Sample <Double> right, boolean text) {
		return text ? similarity(Sample.split(left.getStr()), Sample.split(right.getStr()), left.getLabel(),
			right.getLabel()) :
			similarity(left.getStr(), right.getStr(), left.getLabel(), right.getLabel());
	}

	/**
	 * Similarity = A·B / (|A| * |B|)
	 * Override the calc function of Similarity.
	 */
	@Override
	public double calc(String left, String right) {
		return similarity(left, right, calCosine(left), calCosine(right));
	}

	@Override
	public double calc(String[] left, String[] right) {
		return similarity(left, right, calCosine(left), calCosine(right));
	}

	private <T> double similarity(T left, T right, double lenL, double lenR) {
		double numerator = calCosine(left, right);

		double denominator = Math.sqrt(lenL * lenR);

		denominator = denominator == 0. ? 1e-16 : denominator;
		double ret = numerator / (denominator);
		ret = ret > 1.0 ? 1.0 : ret;
		return ret;
	}
}
