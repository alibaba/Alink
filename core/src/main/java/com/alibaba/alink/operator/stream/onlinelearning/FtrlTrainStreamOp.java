package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.onlinelearning.FtrlTrainParams;

/**
 * Stream train operation of FTRL Algorithm.
 */
public class FtrlTrainStreamOp extends BaseOnlineTrainStreamOp <FtrlTrainStreamOp>
	implements FtrlTrainParams <FtrlTrainStreamOp> {

	private static final long serialVersionUID = 3688413917992858013L;

	public FtrlTrainStreamOp(BatchOperator model) throws Exception {
		super(model);
		setLearningKernel(new FtrlLearningKernel());
	}

	public FtrlTrainStreamOp(BatchOperator model, Params params) throws Exception {
		super(model, params);
		setLearningKernel(new FtrlLearningKernel());
	}

	public static class FtrlLearningKernel extends OnlineLearningKernel {
		private static final long serialVersionUID = 5856064547671182071L;
		private Object[] labelValues;
		private double alpha;
		private double beta;
		private double l1;
		private double l2;
		private double[] nParam;
		private double[] zParam;

		@Override
		public void setModelParams(Params params, int localModelSize, Object[] labelValues) {
			nParam = new double[localModelSize];
			zParam = new double[localModelSize];
			this.alpha = params.get(FtrlTrainParams.ALPHA);
			this.beta = params.get(FtrlTrainParams.BETA);
			this.l1 = params.get(FtrlTrainParams.L_1);
			this.l2 = params.get(FtrlTrainParams.L_2);
			this.labelValues = labelValues;
		}

		@Override
		public double[] getFeedbackVar(double[] wx) {
			return new double[] {1 / (1 + Math.exp(-wx[0]))};
		}

		@Override
		public double[] calcLocalWx(double[] coef, Vector vec, int startIdx) {
			double y = 0.0;
			if (vec instanceof SparseVector) {
				int[] indices = ((SparseVector) vec).getIndices();

				for (int i = 0; i < indices.length; ++i) {
					y += ((SparseVector) vec).getValues()[i] * coef[indices[i] - startIdx];
				}
			} else {
				for (int i = 0; i < vec.size(); ++i) {
					y += vec.get(i) * coef[i];
				}
			}
			return new double[] {y};
		}

		@Override
		public void updateModel(double[] coef, Vector vec, double[] wx, long timeInterval,
								int startIdx, Object labelValue) {
			double pred = wx[0];
			double label;
			if (labelValues[0] instanceof Number) {
				label = (Double.valueOf(labelValue.toString())
					.equals(Double.valueOf(labelValues[0].toString()))) ? 1.0 : 0.0;
			} else {
				label = (labelValue.toString().equals(labelValues[0].toString())) ? 1.0 : 0.0;
			}

			if (vec instanceof SparseVector) {
				int[] indices = ((SparseVector) vec).getIndices();
				double[] values = ((SparseVector) vec).getValues();

				for (int i = 0; i < indices.length; ++i) {
					// update zParam nParam
					int id = indices[i] - startIdx;
					double g = (pred - label) * values[i] / Math.sqrt(timeInterval + 1);
					double sigma = (Math.sqrt(nParam[id] + g * g) - Math.sqrt(nParam[id])) / alpha;
					zParam[id] += g - sigma * coef[id];
					nParam[id] += g * g;

					// update model coefficient
					if (Math.abs(zParam[id]) <= l1) {
						coef[id] = 0.0;
					} else {
						coef[id] = ((zParam[id] < 0 ? -1 : 1) * l1 - zParam[id])
							/ ((beta + Math.sqrt(nParam[id]) / alpha + l2));
					}
				}
			} else {
				double[] data = ((DenseVector) vec).getData();

				for (int i = 0; i < data.length; ++i) {
					// update zParam nParam
					double g = (pred - label) * data[i] / Math.sqrt(timeInterval + 1);
					double sigma = (Math.sqrt(nParam[i] + g * g) - Math.sqrt(nParam[i])) / alpha;
					zParam[i] += g - sigma * coef[i];
					nParam[i] += g * g;

					// update model coefficient
					if (Math.abs(zParam[i]) <= l1) {
						coef[i] = 0.0;
					} else {
						coef[i] = ((zParam[i] < 0 ? -1 : 1) * l1 - zParam[i])
							/ ((beta + Math.sqrt(nParam[i]) / alpha + l2));
					}
				}
			}
		}
	}
}
