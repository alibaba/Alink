package com.alibaba.alink.operator.common.tree.parallelcart.loss;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.Arrays;

public final class LambdaLoss implements RankingLossFunc {
	private final static int MAX_TARGET = 31;
	private final static int MAX_POS = 10000;
	private final static double LOG2 = Math.log(2.0);
	private final static double[] GAIN_TABLE = new double[MAX_TARGET];
	private final static double[] DISCOUNT_TABLE = new double[MAX_POS];

	static {
		for (int i = 0; i < MAX_TARGET; ++i) {
			GAIN_TABLE[i] = (1 << i) - 1.0;
		}

		for (int i = 0; i < MAX_POS; ++i) {
			DISCOUNT_TABLE[i] = 1.0 / Math.log(2.0 + i) * LOG2;
		}
	}

	public enum LambdaType {
		DCG,
		NDCG
	}

	private final int atT;
	private final int numIterUpdatePos;
	private final LambdaType lambdaType;

	private final Integer[] sortedIndices;

	private double[] maxDcgInverse;
	private final int[] labelCnt = new int[MAX_TARGET];

	public static final ParamInfo <Integer> AT_T = ParamInfoFactory
		.createParamInfo("atT", Integer.class)
		.setHasDefaultValue(3)
		.build();

	public static final ParamInfo <Integer> NUM_ITER_UPDATE_POS = ParamInfoFactory
		.createParamInfo("numIterUpdatePos", Integer.class)
		.setHasDefaultValue(10)
		.build();

	public LambdaLoss(
		Params params,
		LambdaType lambdaType,
		int[] queryOffset,
		double[] y,
		double[] weights) {
		this.atT = params.get(AT_T);
		this.numIterUpdatePos = params.get(NUM_ITER_UPDATE_POS);
		this.lambdaType = lambdaType;
		sortedIndices = new Integer[y.length];
		initial(queryOffset, y, weights);
	}

	public void initial(
		int[] queryOffset,
		double[] y,
		double[] weights) {

		if (lambdaType != LambdaType.NDCG) {
			return;
		}

		maxDcgInverse = new double[queryOffset.length - 1];

		for (int i = 0; i < queryOffset.length - 1; ++i) {
			int start = queryOffset[i];
			int end = queryOffset[i + 1];
			int size = end - start;

			Arrays.fill(labelCnt, 0);

			for (int j = start; j < end; ++j) {
				labelCnt[(int) y[j]]++;
			}

			int actualAtT = Math.min(size, atT);
			int maxY = MAX_TARGET - 1;

			for (int j = 0; j < actualAtT; ++j) {
				while (maxY > 0 && labelCnt[maxY] == 0) {
					maxY--;
				}

				if (maxY < 0) {
					break;
				}

				maxDcgInverse[i] += GAIN_TABLE[maxY] * DISCOUNT_TABLE[j];
				labelCnt[maxY]--;
			}

			maxDcgInverse[i] = Math.max(1.0, maxDcgInverse[i]);

			if (maxDcgInverse[i] > 0.) {
				maxDcgInverse[i] = 1.0 / maxDcgInverse[i];
			}
		}
	}

	/**
	 * DCG: Gain(.) / Discount(.)
	 * Rel: r_i r_j
	 * Rank: i j
	 * DCG: Gain(r_i) / Discount(i) + Gain(r_j) / Discount(j)
	 * Swapped DCG: Gain(r_j) / Discount(i) + Gain(r_i) / Discount(j)
	 * Delta: Gain(r_i) / Discount(i) + Gain(r_j) / Discount(j) - Gain(r_j) / Discount(i) - Gain(r_i) / Discount(j)
	 * Delta: Gain(r_i) * (1 / Discount(i) - 1 / Discount(j)) + Gain(r_j) * (1 / Discount(j) - 1 / Discount(i))
	 * Delta: (Gain(r_i) - Gain(r_j)) * (1 / Discount(i) - 1 / Discount(j))
	 */
	@Override
	public void gradients(
		int[] queryOffset,
		int queryId,
		int numIter,
		double[] y,
		double[] eta,
		double[] weights,
		double[] gradients,
		double[] hessions) {

		int start = queryOffset[queryId];
		int end = queryOffset[queryId + 1];
		int size = end - start;
		int div = numIter / numIterUpdatePos;

		if (numIter > 0 && numIter % numIterUpdatePos == 0) {
			for (int i = start; i < end; ++i) {
				sortedIndices[i] = i;
			}

			Arrays.sort(
				sortedIndices, start, end,
				(o1, o2) -> Double.compare(eta[o2], eta[o1])
			);
		} else if (numIter == 0) {
			for (int i = start; i < end; ++i) {
				sortedIndices[i] = i;
			}
		}

		Arrays.fill(weights, start, end, 0.0);
		Arrays.fill(gradients, start, end, 0.0);
		Arrays.fill(hessions, start, end, 0.0);

		for (int i = 0; i < size - 1; ++i) {
			int k1 = sortedIndices[start + i];
			int k1y = (int) y[k1];
			for (int j = i + 1; j < size; ++j) {
				int k2 = sortedIndices[start + j];
				int k2y = (int) y[k2];

				if (k1y == k2y) {
					continue;
				}

				int high, highY, highPos, low, lowY, lowPos;

				if (k1y > k2y) {
					high = k1;
					highY = k1y;
					highPos = i;
					low = k2;
					lowY = k2y;
					lowPos = j;
				} else {
					high = k2;
					highY = k2y;
					highPos = j;
					low = k1;
					lowY = k1y;
					lowPos = i;
				}

				double delta;
				if (div == 0) {
					delta = GAIN_TABLE[highY] - GAIN_TABLE[lowY];
				} else {
					delta = (GAIN_TABLE[highY] - GAIN_TABLE[lowY])
						* Math.abs((DISCOUNT_TABLE[highPos] - DISCOUNT_TABLE[lowPos]));
				}

				if (lambdaType == LambdaType.NDCG) {
					delta = delta * maxDcgInverse[queryId];
				}

				double lambda = sigmoid(eta[low] - eta[high]);
				double hession = lambda * (1.0 - lambda);

				lambda *= delta;
				hession *= delta;

				gradients[high] += lambda;
				gradients[low] -= lambda;
				weights[high] += delta;
				weights[low] += delta;
				hessions[high] += hession;
				hessions[low] += hession;
			}
		}
	}

	public static double sigmoid(double x) {
		return 1.0 / (1.0 + Math.exp(-x));
	}
}
