package com.alibaba.alink.operator.common.tree.parallelcart.loss;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.regression.GBRankParams;

import java.util.Arrays;

public final class GBRankLoss implements RankingLossFunc {
	double tau;
	double p;

	public GBRankLoss(Params params, int[] queryOffset, double[] y, double[] weights) {
		tau = params.get(GBRankParams.TAU);
		p = params.get(GBRankParams.P);

		initial(queryOffset, y, weights);
	}

	public void initial(int[] queryOffset, double[] y, double[] weights) {
		Arrays.fill(weights, 0.0);

		int querySize = queryOffset.length - 1;
		for (int i = 0; i < querySize; ++i) {
			int start = queryOffset[i];
			int end = queryOffset[i + 1];
			int size = end - start;

			for (int j = 0; j < size - 1; ++j) {
				for (int z = j + 1; z < size; ++z) {
					int k1y = (int) y[j + start];
					int k2y = (int) y[z + start];

					if (k1y == k2y) {
						continue;
					}

					weights[j + start] += 1.0;
					weights[z + start] += 1.0;
				}
			}
		}
	}

	@Override
	public void gradients(
		int[] queryOffset,
		int queryId,
		int numIter,
		double[] y,
		double[] eta,
		double[] weights,
		double[] gradients,
		double[] hessions
	) {
		int start = queryOffset[queryId];
		int end = queryOffset[queryId + 1];
		int size = end - start;

		Arrays.fill(gradients, start, end, 0.0);
		Arrays.fill(hessions, start, end, 0.0);

		for (int i = 0; i < size - 1; ++i) {
			for (int j = i + 1; j < size; ++j) {
				int k1y = (int) y[i + start];
				int k2y = (int) y[j + start];

				if (k1y == k2y) {
					continue;
				}

				int high, low;
				if (k1y > k2y) {
					high = i;
					low = j;
				} else {
					high = j;
					low = i;
				}

				double labelDiff;
				if (p > 1.0) {
					labelDiff = Math.pow(p, y[high + start]) - Math.pow(p, y[low + start]);
				} else {
					labelDiff = y[high + start] - y[low + start];
				}

				double grad = LambdaLoss.sigmoid(labelDiff * tau - eta[high + start] + eta[low + start]);
				double hession = grad * (1 - grad);

				gradients[high + start] += grad;
				gradients[low + start] -= grad;
				hessions[high + start] += hession;
				hessions[low + start] += hession;
			}
		}
	}
}
