package com.alibaba.alink.operator.common.optim;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LogitLoss;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LossFunction;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.SquareLoss;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.params.recommendation.FmTrainParams;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Local fm optimizer.
 */
public class LocalFmOptimizer {
	private final List <Tuple3 <Double, Double, Vector>> trainData;
	private final int[] dim;
	protected FmDataFormat fmModel = null;
	private final double[] lambda;

	private FmDataFormat sigmaGii;
	private final double learnRate;
	private final LossFunction lossFunc;
	private final int numEpochs;
	private final Task task;

	private final double[] y;
	private final double[] loss;
	private final double[] vx;
	private final double[] v2x2;
	private long oldTime;
	private final double[] lossCurve;
	private double oldLoss = 1.0;

	/**
	 * construct function.
	 *
	 * @param trainData train data.
	 * @param params    parameters for optimizer.
	 */
	public LocalFmOptimizer(List <Tuple3 <Double, Double, Vector>> trainData, Params params) {
		this.numEpochs = params.get(FmTrainParams.NUM_EPOCHS);
		this.trainData = trainData;
		this.y = new double[trainData.size()];
		this.loss = new double[4];
		this.dim = new int[3];
		dim[0] = params.get(FmTrainParams.WITH_INTERCEPT) ? 1 : 0;
		dim[1] = params.get(FmTrainParams.WITH_LINEAR_ITEM) ? 1 : 0;
		dim[2] = params.get(FmTrainParams.NUM_FACTOR);
		vx = new double[dim[2]];
		v2x2 = new double[dim[2]];
		this.lambda = new double[3];
		lambda[0] = params.get(FmTrainParams.LAMBDA_0);
		lambda[1] = params.get(FmTrainParams.LAMBDA_1);
		lambda[2] = params.get(FmTrainParams.LAMBDA_2);
		task = params.get(ModelParamName.TASK);
		this.learnRate = params.get(FmTrainParams.LEARN_RATE);
		oldTime = System.currentTimeMillis();
		if (task.equals(Task.REGRESSION)) {
			double minTarget = -1.0e20;
			double maxTarget = 1.0e20;
			double d = maxTarget - minTarget;
			d = Math.max(d, 1.0);
			maxTarget = maxTarget + d * 0.2;
			minTarget = minTarget - d * 0.2;
			lossFunc = new SquareLoss(maxTarget, minTarget);
		} else {
			lossFunc = new LogitLoss();
		}

		lossCurve = new double[numEpochs * 3];
	}

	/**
	 * initialize fmModel.
	 */
	public void setWithInitFactors(FmDataFormat model) {
		this.fmModel = model;
		int vectorSize = fmModel.factors.length;
		sigmaGii = new FmDataFormat(vectorSize, dim, 0.0);
	}

	/**
	 * optimize Fm problem.
	 *
	 * @return fm model.
	 */
	public Tuple2 <FmDataFormat, double[]> optimize() {
		for (int i = 0; i < numEpochs; ++i) {
			updateFactors();
			calcLossAndEvaluation();
			if (termination(i)) {
				break;
			}
		}
		return Tuple2.of(fmModel, lossCurve);
	}

	/**
	 * Termination function of fm iteration.
	 */
	public boolean termination(int step) {
		lossCurve[3 * step] = loss[0] / loss[1];
		lossCurve[3 * step + 2] = loss[3] / loss[1];
		if (task.equals(Task.BINARY_CLASSIFICATION)) {
			lossCurve[3 * step + 1] = loss[2];

			System.out.println("step : " + step + " loss : "
				+ loss[0] / loss[1] + "  auc : " + loss[2] + " accuracy : "
				+ loss[3] / loss[1] + " time : " + (System.currentTimeMillis()
				- oldTime));
		} else {
			lossCurve[3 * step + 1] = loss[2] / loss[1];
			System.out.println("step : " + step + " loss : "
				+ loss[0] / loss[1] + "  mae : " + loss[2] / loss[1] + " mse : "
				+ loss[3] / loss[1] + " time : " + (System.currentTimeMillis()
				- oldTime));
		}
		oldTime = System.currentTimeMillis();
		if (Math.abs(oldLoss - loss[0] / loss[1]) / oldLoss < 1.0e-6) {
			oldLoss = loss[0] / loss[1];
			return true;
		} else {
			oldLoss = loss[0] / loss[1];
			return false;
		}
	}

	/**
	 * Calculate loss and evaluations.
	 */
	public void calcLossAndEvaluation() {
		double lossSum = 0.;
		for (int i = 0; i < y.length; i++) {
			double yTruth = trainData.get(i).f1;
			double l = lossFunc.l(yTruth, y[i]);
			lossSum += l;
		}

		if (this.task.equals(Task.REGRESSION)) {
			double mae = 0.0;
			double mse = 0.0;
			for (int i = 0; i < y.length; i++) {
				double yDiff = y[i] - trainData.get(i).f1;
				mae += Math.abs(yDiff);
				mse += yDiff * yDiff;
			}
			loss[2] = mae;
			loss[3] = mse;
		} else {
			Integer[] order = new Integer[y.length];
			double correctNum = 0.0;
			for (int i = 0; i < y.length; i++) {
				order[i] = i;
				if (y[i] > 0 && trainData.get(i).f1 > 0.5) {
					correctNum += 1.0;
				}
				if (y[i] < 0 && trainData.get(i).f1 < 0.5) {
					correctNum += 1.0;
				}
			}
			Arrays.sort(order, Comparator.comparingDouble(o -> y[o]));
			int mSum = 0;
			int nSum = 0;
			double posRankSum = 0.;
			for (int i = 0; i < order.length; i++) {
				int sampleId = order[i];
				int rank = i + 1;
				boolean isPositiveSample = trainData.get(sampleId).f1 > 0.5;
				if (isPositiveSample) {
					mSum++;
					posRankSum += rank;
				} else {
					nSum++;
				}
			}
			if (mSum != 0 && nSum != 0) {
				double auc = (posRankSum - 0.5 * mSum * (mSum + 1.0)) / ((double) mSum * (double) nSum);
				loss[2] = auc;
			} else {
				loss[2] = 0.0;
			}
			loss[3] = correctNum;
		}
		loss[0] = lossSum;
		loss[1] = y.length;
	}

	private void updateFactors() {
		for (int i1 = 0; i1 < trainData.size(); ++i1) {
			Tuple3 <Double, Double, Vector> sample = trainData.get(i1);
			Vector vec = sample.f2;
			Tuple2 <Double, double[]> yVx = calcY(vec, fmModel, dim);
			y[i1] = yVx.f0;

			double yTruth = sample.f1;
			double dldy = lossFunc.dldy(yTruth, yVx.f0);

			int[] indices;
			double[] vals;
			if (sample.f2 instanceof SparseVector) {
				indices = ((SparseVector) sample.f2).getIndices();
				vals = ((SparseVector) sample.f2).getValues();
			} else {
				indices = new int[sample.f2.size()];
				for (int i = 0; i < sample.f2.size(); ++i) {
					indices[i] = i;
				}
				vals = ((DenseVector) sample.f2).getData();
			}
			double localLearnRate = sample.f0 * learnRate;

			double eps = 1.0e-8;
			if (dim[0] > 0) {
				double grad = dldy + lambda[0] * fmModel.bias;
				sigmaGii.bias += grad * grad;
				fmModel.bias -= localLearnRate * grad / (Math.sqrt(sigmaGii.bias + eps));
			}

			for (int i = 0; i < indices.length; ++i) {
				int idx = indices[i];
				// update fmModel
				for (int j = 0; j < dim[2]; j++) {
					double vixi = vals[i] * fmModel.factors[idx][j];
					double d = vals[i] * (yVx.f1[j] - vixi);
					double grad = dldy * d + lambda[2] * fmModel.factors[idx][j];
					sigmaGii.factors[idx][j] += grad * grad;
					fmModel.factors[idx][j] -= localLearnRate * grad / (Math.sqrt(sigmaGii.factors[idx][j] + eps));
				}
				if (dim[1] > 0) {
					double grad = dldy * vals[i] + lambda[1] * fmModel.factors[idx][dim[2]];
					sigmaGii.factors[idx][dim[2]] += grad * grad;
					fmModel.factors[idx][dim[2]]
						-= grad * localLearnRate / (Math.sqrt(sigmaGii.factors[idx][dim[2]]+ eps));
				}
			}
		}
	}

	/**
	 * calculate the value of y with given fm model.
	 */
	private Tuple2 <Double, double[]> calcY(Vector vec, FmDataFormat fmModel, int[] dim) {
		int[] featureIds;
		double[] featureValues;
		if (vec instanceof SparseVector) {
			featureIds = ((SparseVector) vec).getIndices();
			featureValues = ((SparseVector) vec).getValues();
		} else {
			featureIds = new int[vec.size()];
			for (int i = 0; i < vec.size(); ++i) {
				featureIds[i] = i;
			}
			featureValues = ((DenseVector) vec).getData();
		}

		Arrays.fill(vx, 0.0);
		Arrays.fill(v2x2, 0.0);

		// (1) compute y
		double y = 0.;

		if (dim[0] > 0) {
			y += fmModel.bias;
		}

		for (int i = 0; i < featureIds.length; i++) {
			int featurePos = featureIds[i];
			double x = featureValues[i];

			// the linear term
			if (dim[1] > 0) {
				y += x * fmModel.factors[featurePos][dim[2]];
			}
			// the quadratic term
			for (int j = 0; j < dim[2]; j++) {
				double vixi = x * fmModel.factors[featurePos][j];
				vx[j] += vixi;
				v2x2[j] += vixi * vixi;
			}
		}

		for (int i = 0; i < dim[2]; i++) {
			y += 0.5 * (vx[i] * vx[i] - v2x2[i]);
		}
		return Tuple2.of(y, vx);
	}
}