package com.alibaba.alink.operator.common.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.exceptions.AkIllegalStateException;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Criteria.
 */
public abstract class Criteria implements Cloneable, Serializable {
	public static final double INVALID_GAIN = 0.0;
	public static final double EPS = 1e-15;
	private static final long serialVersionUID = -4855396890380900139L;

	protected double weightSum;
	protected int numInstances;

	public Criteria(double weightSum, int numInstances) {
		this.weightSum = weightSum;
		this.numInstances = numInstances;
	}

	public abstract LabelCounter toLabelCounter();

	public abstract double impurity();

	public abstract double gain(Criteria... children);

	public abstract Criteria add(Criteria other);

	public abstract Criteria subtract(Criteria other);

	public double getWeightSum() {
		return weightSum;
	}

	public int getNumInstances() {
		return numInstances;
	}

	public abstract void reset();

	@Override
	public Criteria clone() {
		try {
			return (Criteria) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AkIllegalStateException("Can not clone the criteria.", e);
		}
	}

	public static abstract class ClassificationCriteria extends Criteria {
		private static final long serialVersionUID = -631328604371947566L;
		double[] distributions;

		ClassificationCriteria(double weightSum, int numInstances, double[] distributions) {
			super(weightSum, numInstances);
			this.distributions = distributions;
		}

		public void add(int labelValue, double weight, int numInstances) {
			this.distributions[labelValue] += weight;
			this.weightSum += weight;
			this.numInstances += numInstances;
		}

		public void subtract(int labelValue, double weight, int numInstance) {
			this.distributions[labelValue] -= weight;
			this.weightSum -= weight;
			this.numInstances -= numInstance;
		}

		@Override
		public ClassificationCriteria add(Criteria other) {
			ClassificationCriteria classificationCriteria = (ClassificationCriteria) other;
			for (int i = 0; i < distributions.length; ++i) {
				distributions[i] += classificationCriteria.distributions[i];
			}

			this.weightSum += classificationCriteria.weightSum;
			this.numInstances += classificationCriteria.numInstances;

			return this;
		}

		@Override
		public Criteria subtract(Criteria other) {
			ClassificationCriteria classificationCriteria = (ClassificationCriteria) other;
			for (int i = 0; i < distributions.length; ++i) {
				distributions[i] -= classificationCriteria.distributions[i];
			}

			this.weightSum -= classificationCriteria.weightSum;
			this.numInstances -= classificationCriteria.numInstances;

			return this;
		}

		@Override
		public ClassificationCriteria clone() {
			ClassificationCriteria criteria = (ClassificationCriteria) super.clone();
			criteria.distributions = distributions.clone();
			criteria.weightSum = weightSum;
			criteria.numInstances = numInstances;
			return criteria;
		}

		@Override
		public LabelCounter toLabelCounter() {
			return new LabelCounter(weightSum, numInstances, distributions);
		}

		@Override
		public void reset() {
			Arrays.fill(distributions, 0.0);
			weightSum = 0.0;
			numInstances = 0;
		}
	}

	public static abstract class RegressionCriteria extends Criteria {
		private static final long serialVersionUID = -2896832363044118789L;

		public RegressionCriteria(double weightSum, int numInstances) {
			super(weightSum, numInstances);
		}

		public abstract void add(double labelValue, double weight, int numInstance);

		public abstract void subtract(double labelValue, double weight, int numInstance);
	}

	public static class Gini extends ClassificationCriteria {
		private static final long serialVersionUID = 8996209222867178997L;

		public Gini(double weightSum, int numInstances, double[] distributions) {
			super(weightSum, numInstances, distributions);
		}

		@Override
		public double impurity() {
			if (weightSum < EPS) {
				return 0.;
			}

			double powP = 0.;

			for (double curStat : distributions) {
				double p = curStat / weightSum;
				powP += p * p;
			}

			return 1. - powP;
		}

		@Override
		public double gain(Criteria... children) {
			if (weightSum < EPS) {
				return INVALID_GAIN;
			}

			double g = impurity();

			for (Criteria gini : children) {
				g -= gini.weightSum / weightSum * gini.impurity();
			}

			return g;
		}
	}

	public abstract static class Entropy extends ClassificationCriteria {
		private final static double LOG2 = Math.log(2);
		private static final long serialVersionUID = 7602253844112062279L;

		Entropy(double weightSum, int numInstances, double[] distributions) {
			super(weightSum, numInstances, distributions);
		}

		static double log2(double d) {
			if (d == 0.) {
				return 0.;
			}

			return Math.log(d) / LOG2;
		}

		@Override
		public double impurity() {
			if (weightSum < EPS) {
				return 0.;
			}

			double entropy = 0.;

			for (int i = 0; i < distributions.length; ++i) {
				double curStat = distributions[i];
				double p = curStat / weightSum;
				entropy += p * log2(p);
			}

			return -1.0 * entropy;
		}
	}

	public static class InfoGain extends Entropy {
		private static final long serialVersionUID = 5185893562589077621L;

		public InfoGain(double weightSum, int numInstances, double[] distributions) {
			super(weightSum, numInstances, distributions);
		}

		@Override
		public double gain(Criteria... children) {
			if (weightSum < EPS) {
				return INVALID_GAIN;
			}

			double g = impurity();

			for (Criteria entropy : children) {
				g -= entropy.weightSum / weightSum * entropy.impurity();
			}

			return g;
		}
	}

	public static class InfoGainRatio extends Entropy {
		private static final long serialVersionUID = 2010844081408314373L;

		public InfoGainRatio(double weightSum, int numInstances, double[] distributions) {
			super(weightSum, numInstances, distributions);
		}

		@Override
		public double gain(Criteria... children) {
			if (weightSum < EPS) {
				return INVALID_GAIN;
			}

			double g = impurity();
			double intrinsicValue = 0.;

			for (Criteria entropy : children) {
				double p = entropy.weightSum / weightSum;
				g -= p * entropy.impurity();
				intrinsicValue -= p * log2(p);
			}

			if (intrinsicValue < EPS) {
				return INVALID_GAIN;
			}

			return g / intrinsicValue;
		}
	}

	public static class MSE extends RegressionCriteria {
		private static final long serialVersionUID = 8895470577519000835L;
		double sum;
		double squareSum;

		public MSE(double weightSum, int numInstances, double sum, double squareSum) {
			super(weightSum, numInstances);
			this.sum = sum;
			this.squareSum = squareSum;
		}

		public double getSum() {
			return sum;
		}

		public void setSum(double sum) {
			this.sum = sum;
		}

		public double getSquareSum() {
			return squareSum;
		}

		public void setSquareSum(double squareSum) {
			this.squareSum = squareSum;
		}

		public void add(double labelValue, double weight, int numInstances) {
			double lw = labelValue * weight;
			this.sum += lw;
			this.squareSum += lw * lw;
			this.weightSum += weight;
			this.numInstances += numInstances;
		}

		public void subtract(double labelValue, double weight, int numInstances) {
			double lw = labelValue * weight;
			this.sum -= lw;
			this.squareSum -= lw * lw;
			this.weightSum -= weight;
			this.numInstances -= numInstances;
		}

		@Override
		public MSE add(Criteria other) {
			MSE mse = (MSE) other;
			this.sum += mse.sum;
			this.squareSum += mse.squareSum;
			this.weightSum += mse.weightSum;
			this.numInstances += mse.numInstances;
			return this;
		}

		@Override
		public Criteria subtract(Criteria other) {
			MSE mse = (MSE) other;
			this.sum -= mse.sum;
			this.squareSum -= mse.squareSum;
			this.weightSum -= mse.weightSum;
			this.numInstances -= mse.numInstances;
			return this;
		}

		@Override
		public void reset() {
			this.sum = 0.0;
			this.squareSum = 0.0;
			this.weightSum = 0.0;
			this.numInstances = 0;
		}

		@Override
		public LabelCounter toLabelCounter() {
			return new LabelCounter(weightSum, numInstances, new double[] {sum, squareSum});
		}

		@Override
		public double impurity() {
			if (getWeightSum() < EPS) {
				return 0.;
			}

			double mean = sum / weightSum;

			return squareSum / weightSum - mean * mean;
		}

		@Override
		public double gain(Criteria... children) {
			if (weightSum < EPS) {
				return INVALID_GAIN;
			}

			double g = impurity();

			for (Criteria mse : children) {
				g -= mse.weightSum / weightSum * mse.impurity();
			}

			return g;
		}
	}

	public static boolean isRegression(TreeUtil.TreeType treeType) {
		switch (treeType) {
			case MSE:
				return true;
			case AVG:
			case PARTITION:
			case GINI:
			case INFOGAIN:
			case INFOGAINRATIO:
				return false;
			default:
				throw new AkIllegalArgumentException("Not support " + treeType + " yet.");
		}
	}

	public enum Gain {
		GINI,
		INFOGAIN,
		INFOGAINRATIO,
		MSE;

		public static final ParamInfo <Gain> GAIN = ParamInfoFactory
			.createParamInfo("gain", Gain.class)
			.build();
	}
}
