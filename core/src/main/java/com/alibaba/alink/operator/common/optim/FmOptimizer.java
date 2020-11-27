package com.alibaba.alink.operator.common.optim;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LogitLoss;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LossFunction;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.SquareLoss;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.Task;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.params.recommendation.FmTrainParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Fm optimizer.
 */
public class FmOptimizer {
	private Params params;
	private DataSet <Tuple3 <Double, Double, Vector>> trainData;
	private int[] dim;
	protected DataSet <FmDataFormat> fmModel = null;
	private double[] lambda;

	/**
	 * construct function.
	 *
	 * @param trainData train data.
	 * @param params    parameters for optimizer.
	 */
	public FmOptimizer(DataSet <Tuple3 <Double, Double, Vector>> trainData, Params params) {
		this.params = params;
		this.trainData = trainData;

		this.dim = new int[3];
		dim[0] = params.get(FmTrainParams.WITH_INTERCEPT) ? 1 : 0;
		dim[1] = params.get(FmTrainParams.WITH_LINEAR_ITEM) ? 1 : 0;
		dim[2] = params.get(FmTrainParams.NUM_FACTOR);

		this.lambda = new double[3];
		lambda[0] = params.get(FmTrainParams.LAMBDA_0);
		lambda[1] = params.get(FmTrainParams.LAMBDA_1);
		lambda[2] = params.get(FmTrainParams.LAMBDA_2);
	}

	/**
	 * initialize fmModel.
	 */
	public void setWithInitFactors(DataSet <FmDataFormat> model) {
		this.fmModel = model;
	}

	/**
	 * optimize Fm problem.
	 *
	 * @return fm model.
	 */
	public DataSet <Tuple2 <FmDataFormat, double[]>> optimize() {
		DataSet <Row> model = new IterativeComQueue()
			.initWithPartitionedData(OptimVariable.fmTrainData, trainData)
			.initWithBroadcastData(OptimVariable.fmModel, fmModel)
			.add(new UpdateLocalModel(dim, lambda, params))
			.add(new AllReduce(OptimVariable.factorAllReduce))
			.add(new UpdateGlobalModel(dim))
			.add(new CalcLossAndEvaluation(dim, params.get(ModelParamName.TASK)))
			.add(new AllReduce(OptimVariable.lossAucAllReduce))
			.setCompareCriterionOfNode0(new FmIterTermination(params))
			.closeWith(new OutputFmModel())
			.setMaxIter(Integer.MAX_VALUE)
			.exec();
		return model.mapPartition(new ParseRowModel());
	}

	/**
	 * Termination class of fm iteration.
	 */
	public static class FmIterTermination extends CompareCriterionFunction {
		private static final long serialVersionUID = 2410437704683855923L;
		private double oldLoss;
		private double epsilon;
		private Task task;
		private int maxIter;
		private int batchSize;
		private Long oldTime;

		public FmIterTermination(Params params) {
			this.maxIter = params.get(FmTrainParams.NUM_EPOCHS);
			this.epsilon = params.get(FmTrainParams.EPSILON);
			this.task = Task.valueOf(params.get(ModelParamName.TASK).toUpperCase());
			this.batchSize = params.get(FmTrainParams.MINIBATCH_SIZE);
			this.oldTime = System.currentTimeMillis();
		}

		@Override
		public boolean calc(ComContext context) {
			int numSamplesOnNode0
				= ((List <Tuple3 <Double, Double, Vector>>) context.getObj(OptimVariable.fmTrainData)).size();

			int numBatches = (batchSize == -1 || batchSize > numSamplesOnNode0) ? maxIter
				: (numSamplesOnNode0 / batchSize + 1) * maxIter;

			double[] lossCurve = context.getObj(OptimVariable.convergenceInfo);
			if (lossCurve == null) {
				lossCurve = new double[numBatches * 3];
				context.putObj(OptimVariable.convergenceInfo, lossCurve);
			}

			int step = context.getStepNo() - 1;
			double[] loss = context.getObj(OptimVariable.lossAucAllReduce);
			lossCurve[3 * step] = loss[0] / loss[1];
			lossCurve[3 * step + 2] = loss[3] / loss[1];
			if (task.equals(Task.BINARY_CLASSIFICATION)) {
				lossCurve[3 * step + 1] = loss[2] / context.getNumTask();
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("step : " + step + " loss : "
						+ loss[0] / loss[1] + "  auc : " + loss[2] / context.getNumTask() + " accuracy : "
						+ loss[3] / loss[1] + " time : " + (System.currentTimeMillis()
						- oldTime));
					oldTime = System.currentTimeMillis();
				}
			} else {
				lossCurve[3 * step + 1] = loss[2] / loss[1];
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("step : " + step + " loss : "
						+ loss[0] / loss[1] + "  mae : " + loss[2] / loss[1] + " mse : "
						+ loss[3] / loss[1] + " time : " + (System.currentTimeMillis()
						- oldTime));
					oldTime = System.currentTimeMillis();
				}
			}

			if (context.getStepNo() == numBatches) {
				return true;
			}

			if (Math.abs(oldLoss - loss[0] / loss[1]) / oldLoss < epsilon) {
				return true;
			} else {
				oldLoss = loss[0] / loss[1];
				return false;
			}
		}
	}

	/**
	 * Calculate loss and evaluations.
	 */
	public static class CalcLossAndEvaluation extends ComputeFunction {
		private static final long serialVersionUID = 1276524768860519162L;
		private int[] dim;
		private double[] y;
		private LossFunction lossFunc = null;
		private Task task;

		public CalcLossAndEvaluation(int[] dim, String task) {
			this.dim = dim;
			this.task = Task.valueOf(task.toUpperCase());
			if (this.task.equals(Task.REGRESSION)) {
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
		}

		@Override
		public void calc(ComContext context) {
			double[] buffer = context.getObj(OptimVariable.lossAucAllReduce);
			// prepare buffer vec for allReduce. the last element of vec is the weight Sum.
			if (buffer == null) {
				buffer = new double[4];
				context.putObj(OptimVariable.lossAucAllReduce, buffer);
			}
			List <Tuple3 <Double, Double, Vector>> labledVectors = context.getObj(OptimVariable.fmTrainData);
			if (this.y == null) {
				this.y = new double[labledVectors.size()];
			}
			// get fmModel from static memory.
			FmDataFormat factors = ((List <FmDataFormat>) context.getObj(OptimVariable.fmModel)).get(0);
			Arrays.fill(y, 0.0);
			for (int s = 0; s < labledVectors.size(); s++) {
				Vector sample = labledVectors.get(s).f2;
				y[s] = calcY(sample, factors, dim).f0;
			}
			double lossSum = 0.;
			for (int i = 0; i < y.length; i++) {
				double yTruth = labledVectors.get(i).f1;
				double l = lossFunc.l(yTruth, y[i]);
				lossSum += l;
			}

			if (this.task.equals(Task.REGRESSION)) {
				double mae = 0.0;
				double mse = 0.0;
				for (int i = 0; i < y.length; i++) {
					double yDiff = y[i] - labledVectors.get(i).f1;
					mae += Math.abs(yDiff);
					mse += yDiff * yDiff;
				}
				buffer[2] = mae;
				buffer[3] = mse;
			} else {
				Integer[] order = new Integer[y.length];
				double correctNum = 0.0;
				for (int i = 0; i < y.length; i++) {
					order[i] = i;
					if (y[i] > 0 && labledVectors.get(i).f1 > 0.5) {
						correctNum += 1.0;
					}
					if (y[i] < 0 && labledVectors.get(i).f1 < 0.5) {
						correctNum += 1.0;
					}
				}
				Arrays.sort(order, new java.util.Comparator <Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return Double.compare(y[o1], y[o2]);
					}
				});
				int mSum = 0;
				int nSum = 0;
				double posRankSum = 0.;
				for (int i = 0; i < order.length; i++) {
					int sampleId = order[i];
					int rank = i + 1;
					boolean isPositiveSample = labledVectors.get(sampleId).f1 > 0.5;
					if (isPositiveSample) {
						mSum++;
						posRankSum += rank;
					} else {
						nSum++;
					}
				}
				if (mSum != 0 && nSum != 0) {
					double auc = (posRankSum - 0.5 * mSum * (mSum + 1.0)) / ((double) mSum * (double) nSum);
					buffer[2] = auc;
					buffer[3] = correctNum;
				} else {
					buffer[2] = 0.0;
					buffer[3] = correctNum;
				}
			}
			buffer[0] = lossSum;
			buffer[1] = y.length;
		}
	}

	/**
	 * Update global fm model.
	 */
	public static class UpdateGlobalModel extends ComputeFunction {
		private static final long serialVersionUID = 4584059654350995646L;
		private int[] dim;

		public UpdateGlobalModel(int[] dim) {
			this.dim = dim;
		}

		@Override
		public void calc(ComContext context) {
			double[] buffer = context.getObj(OptimVariable.factorAllReduce);
			FmDataFormat sigmaGii = context.getObj(OptimVariable.sigmaGii);
			FmDataFormat factors = ((List <FmDataFormat>) context.getObj(OptimVariable.fmModel)).get(0);

			int vectorSize = (buffer.length - 2 * dim[0]) / (2 * dim[2] + 2 * dim[1] + 1);
			int jLen = dim[2] + dim[1];
			for (int i = 0; i < vectorSize; ++i) {
				double weightSum = buffer[2 * vectorSize * jLen + i];
				if (weightSum > 0.0) {
					for (int j = 0; j < dim[2]; ++j) {
						factors.factors[i][j] = buffer[i * dim[2] + j] / weightSum;
						sigmaGii.factors[i][j] = buffer[(vectorSize + i) * dim[2] + j] / weightSum;
					}
					if (dim[1] > 0) {
						factors.linearItems[i] = buffer[vectorSize * dim[2] * 2 + i] / weightSum;
						sigmaGii.linearItems[i] = buffer[vectorSize * (dim[2] * 2 + 1) + i] / weightSum;
					}
				}
			}
			if (dim[0] > 0) {
				factors.bias = buffer[buffer.length - 2] / context.getNumTask();
				sigmaGii.bias = buffer[buffer.length - 1] / context.getNumTask();
			}
		}
	}

	/**
	 * Update local fm model.
	 */
	public static class UpdateLocalModel extends ComputeFunction {

		private static final long serialVersionUID = 5331512619834061299L;
		/**
		 * object function class, it supply the functions to calc local gradient (or loss).
		 */
		private int[] dim;
		private double[] lambda;
		private Task task;
		private double learnRate;
		private int vectorSize;
		private final double eps = 1.0e-8;
		private int batchSize;
		private LossFunction lossFunc = null;
		private Random rand = new Random(2020);

		public UpdateLocalModel(int[] dim, double[] lambda, Params params) {
			this.lambda = lambda;
			this.dim = dim;
			this.task = Task.valueOf(params.get(ModelParamName.TASK).toUpperCase());
			this.learnRate = params.get(FmTrainParams.LEARN_RATE);
			this.batchSize = params.get(FmTrainParams.MINIBATCH_SIZE);
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
		}

		@Override
		public void calc(ComContext context) {
			ArrayList <Tuple3 <Double, Double, Vector>> labledVectors = context.getObj(OptimVariable.fmTrainData);
			if (batchSize == -1) {
				batchSize = labledVectors.size();
			}
			// get fmModel from static memory.
			FmDataFormat sigmaGii = context.getObj(OptimVariable.sigmaGii);
			FmDataFormat innerModel = ((List <FmDataFormat>) context.getObj(OptimVariable.fmModel)).get(0);
			double[] weights = context.getObj(OptimVariable.weights);
			if (weights == null) {
				vectorSize = (innerModel.factors != null) ? innerModel.factors.length : innerModel.linearItems.length;
				weights = new double[vectorSize];
				context.putObj(OptimVariable.weights, weights);
			} else {
				Arrays.fill(weights, 0.);
			}

			if (sigmaGii == null) {
				sigmaGii = new FmDataFormat(vectorSize, dim, 0.0);
				context.putObj(OptimVariable.sigmaGii, sigmaGii);
			}

			updateFactors(labledVectors, innerModel, learnRate, sigmaGii, weights);

			// prepare buffer vec for allReduce. the last element of vec is the weight Sum.
			double[] buffer = context.getObj(OptimVariable.factorAllReduce);
			if (buffer == null) {
				buffer = new double[vectorSize * (dim[1] + dim[2]) * 2 + vectorSize + 2 * dim[0]];
				context.putObj(OptimVariable.factorAllReduce, buffer);
			} else {
				Arrays.fill(buffer, 0.0);
			}

			for (int i = 0; i < vectorSize; ++i) {
				for (int j = 0; j < dim[2]; ++j) {
					buffer[i * dim[2] + j] = innerModel.factors[i][j] * weights[i];
					buffer[(vectorSize + i) * dim[2] + j] = sigmaGii.factors[i][j] * weights[i];
				}
				if (dim[1] > 0) {
					buffer[vectorSize * dim[2] * 2 + i] = innerModel.linearItems[i] * weights[i];
					buffer[vectorSize * (dim[2] * 2 + dim[1]) + i] = sigmaGii.linearItems[i] * weights[i];
				}
				buffer[vectorSize * ((dim[2] + dim[1]) * 2) + i] = weights[i];
			}
			if (dim[0] > 0) {
				buffer[vectorSize * ((dim[2] + dim[1]) * 2 + 1)] = innerModel.bias;
				buffer[vectorSize * ((dim[2] + dim[1]) * 2 + 1) + 1] = sigmaGii.bias;
			}
		}

		private void updateFactors(List <Tuple3 <Double, Double, Vector>> labledVectors,
								   FmDataFormat factors,
								   double learnRate,
								   FmDataFormat sigmaGii,
								   double[] weights) {
			for (int bi = 0; bi < batchSize * 2; ++bi) {
				Tuple3 <Double, Double, Vector> sample = labledVectors.get(rand.nextInt(labledVectors.size()));
				Vector vec = sample.f2;
				Tuple2 <Double, double[]> yVx = calcY(vec, factors, dim);
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

				if (dim[0] > 0) {
					double grad = dldy + lambda[0] * factors.bias;

					sigmaGii.bias += grad * grad;
					factors.bias += -learnRate * grad / (Math.sqrt(sigmaGii.bias + eps));
				}

				for (int i = 0; i < indices.length; ++i) {
					int idx = indices[i];

					weights[idx] += sample.f0;
					// update fmModel
					for (int j = 0; j < dim[2]; j++) {
						double vixi = vals[i] * factors.factors[idx][j];
						double d = vals[i] * (yVx.f1[j] - vixi);
						double grad = dldy * d + lambda[2] * factors.factors[idx][j];
						sigmaGii.factors[idx][j] += grad * grad;
						factors.factors[idx][j] += -learnRate * grad / (Math.sqrt(sigmaGii.factors[idx][j] + eps));
					}
					if (dim[1] > 0) {
						double grad = dldy * vals[i] + lambda[1] * factors.linearItems[idx];
						sigmaGii.linearItems[idx] += grad * grad;
						factors.linearItems[idx] += -grad * learnRate / (Math.sqrt(sigmaGii.linearItems[idx] + eps));
					}
				}
			}
		}
	}

	/**
	 * Output fm model with row format.
	 */
	public static class OutputFmModel extends CompleteResultFunction {

		private static final long serialVersionUID = 727259322769437038L;

		@Override
		public List <Row> calc(ComContext context) {
			if (context.getTaskId() != 0) {
				return null;
			}
			double[] lossCurve = context.getObj(OptimVariable.convergenceInfo);

			FmDataFormat factors = ((List <FmDataFormat>) context.getObj(OptimVariable.fmModel)).get(0);
			List <Row> model = new ArrayList <>();
			model.add(Row.of(0, JsonConverter.toJson(factors)));
			model.add(Row.of(1, JsonConverter.toJson(lossCurve)));
			return model;
		}
	}

	public class ParseRowModel extends RichMapPartitionFunction <Row, Tuple2 <FmDataFormat, double[]>> {
		private static final long serialVersionUID = -2078134573230730223L;

		@Override
		public void mapPartition(Iterable <Row> iterable,
								 Collector <Tuple2 <FmDataFormat, double[]>> collector) throws Exception {
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			if (taskId == 0) {
				FmDataFormat factor = new FmDataFormat();
				double[] cinfo = new double[0];
				for (Row row : iterable) {
					if ((int) row.getField(0) == 0) {
						factor = JsonConverter.fromJson((String) row.getField(1), FmDataFormat.class);
					} else {
						cinfo = JsonConverter.fromJson((String) row.getField(1), double[].class);
					}
				}
				collector.collect(Tuple2.of(factor, cinfo));
			}
		}
	}

	/**
	 * calculate the value of y with given fm model.
	 *
	 * @param vec
	 * @param fmModel
	 * @param dim
	 * @return
	 */
	public static Tuple2 <Double, double[]> calcY(Vector vec, FmDataFormat fmModel, int[] dim) {
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

		double[] vx = new double[dim[2]];
		double[] v2x2 = new double[dim[2]];

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
				y += x * fmModel.linearItems[featurePos];
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