package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.SimilarityUtil;

/**
 * Calculate the string subsequence kernel.
 * SSK: maps strings to a feature vector indexed by all k tuples of characters, and
 * get the dot product.
 */
public class SubsequenceKernelSimilarity extends Similarity <Double> {
	private static final long serialVersionUID = 5169302696020497666L;
	private int k;
	private double lambda;

	public SubsequenceKernelSimilarity(int k, double lambda) {
		if (k < 0) {
			throw new RuntimeException("k must be positive!");
		}
		this.k = k;
		this.lambda = lambda;
		this.distance = null;
	}

	<T> double sskLinearSpace(T lsv, T rsv, double[][][] kp) {
		if (lsv instanceof String) {
			String left = (String) lsv;
			String right = (String) rsv;
			int ll = left.length();
			int lr = right.length();
			if (ll < this.k || lr < this.k) {
				return 0.0;
			}
			if (null == kp) {
				kp = new double[this.k + 1][2][lr];
			}
			//memory consumption is sizeof(long) * 2 * k * lr
			if ((long) lr * 2 * (long) this.k > SimilarityUtil.MAX_MEMORY) {
				throw new RuntimeException("String is Too Long for SSK, please use other method");
			}
			int newIndex = 1;  //newIndex=new line, 1-newIndex=old line
			double ret = 0.;
			for (int j = 0; j < ll - 1; j++) {
				int oldIndex = 1 - newIndex;
				for (int z = 0; z < lr; z++) {
					kp[0][oldIndex][z] = 1.0;
				}
				if (j == 0) {
					for (int i = 1; i < this.k; i++) {
						for (int z = 0; z < lr; z++) {
							kp[i][oldIndex][z] = 0.0;
						}
					}
				}
				for (int i = 0; i < this.k; i++) {
					double kpp = 0.0;
					for (int z = 0; z < lr - 1; z++) {
						kpp = this.lambda * (kpp + this.lambda * (left.charAt(j) == right.charAt(z) ? 1 : 0)
							* kp[i][oldIndex][z]);
						kp[i + 1][newIndex][z + 1] = this.lambda * kp[i + 1][oldIndex][z + 1] + kpp;
					}
				}

				//count ret here
				for (int z = 0; z < lr; z++) {
					ret += lambda * lambda * (left.charAt(j + 1) == right.charAt(z) ? 1 : 0) * kp[k - 1][newIndex][z];
				}
				newIndex = oldIndex;
			}

			return ret;
		} else {
			String[] left = (String[]) lsv;
			String[] right = (String[]) rsv;
			int ll = left.length;
			int lr = right.length;
			if (ll < this.k || lr < this.k) {
				return 0.0;
			}
			if (null == kp) {
				kp = new double[this.k + 1][2][lr];
			}
			//memory consumption is sizeof(long) * 2 * k * lr
			if ((long) lr * 2 * (long) this.k > SimilarityUtil.MAX_MEMORY) {
				throw new RuntimeException("String is Too Long for SSK, please use other method");
			}
			int newIndex = 1;  //newIndex=new line, 1-newIndex=old line
			double ret = 0.;
			for (int j = 0; j < ll - 1; j++) {
				int oldIndex = 1 - newIndex;
				for (int z = 0; z < lr; z++) {
					kp[0][oldIndex][z] = 1.0;
				}
				if (j == 0) {
					for (int i = 1; i < this.k; i++) {
						for (int z = 0; z < lr; z++) {
							kp[i][oldIndex][z] = 0.0;
						}
					}
				}
				for (int i = 0; i < this.k; i++) {
					double kpp = 0.0;
					for (int z = 0; z < lr - 1; z++) {
						kpp = this.lambda * (kpp + this.lambda * (left[j].equals(right[z]) ? 1 : 0)
							* kp[i][oldIndex][z]);
						kp[i + 1][newIndex][z + 1] = this.lambda * kp[i + 1][oldIndex][z + 1] + kpp;
					}
				}

				//count ret here
				for (int z = 0; z < lr; z++) {
					ret += lambda * lambda * (left[j + 1].equals(right[z]) ? 1 : 0) * kp[k - 1][newIndex][z];
				}
				newIndex = oldIndex;
			}

			return ret;
		}
	}

	/**
	 * Similarity = SSK(A,B) / (SSK(A,A) * SSK(B,B))
	 * Override the calc function of Similarity.
	 */
	@Override
	public double calc(String left, String right) {
		return similarity(left, right, sskLinearSpace(left, left, null), sskLinearSpace(right, right, null));
	}

	@Override
	public double calc(String[] left, String[] right) {
		return similarity(left, right, sskLinearSpace(left, left, null), sskLinearSpace(right, right, null));
	}

	@Override
	public <M> void updateLabel(Sample sample, M str) {
		sample.setLabel(sskLinearSpace(str, str, null));
	}

	@Override
	public double calc(Sample <Double> left, Sample <Double> right, boolean text) {
		double numerator = text ? sskLinearSpace(Sample.split(left.getStr()),
			Sample.split(right.getStr()), null) : sskLinearSpace(left.getStr(), right.getStr(), null);
		double denominator = Math.sqrt(left.getLabel() * right.getLabel());
		denominator = denominator == 0. ? 1e-16 : denominator;
		double ret = numerator / denominator;

		ret = ret > 1.0 ? 1.0 : ret;
		return ret;
	}

	private <T> double similarity(T left, T right, double lenL, double lenR) {
		double numerator = sskLinearSpace(left, right, null);
		double denominator = Math.sqrt(lenL * lenR);
		denominator = denominator == 0. ? 1e-16 : denominator;
		double ret = numerator / denominator;

		ret = ret > 1.0 ? 1.0 : ret;
		return ret;
	}
}
