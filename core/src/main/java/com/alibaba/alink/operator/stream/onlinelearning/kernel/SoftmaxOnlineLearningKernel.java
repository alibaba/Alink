package com.alibaba.alink.operator.stream.onlinelearning.kernel;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.onlinelearning.OnlineLearningTrainParams.OptimMethod;

import java.util.List;

public class SoftmaxOnlineLearningKernel extends LinearOnlineLearningKernel {
	private int k1;

	public SoftmaxOnlineLearningKernel(Params params) {
		super(params, LinearModelType.LR);
	}

	@Override
	public void calcGradient(Vector vec, Object label) throws Exception {
		int yk = 0;
		for (int i = 0; i < modelData.labelValues.length; ++i) {
			if (label.equals(modelData.labelValues[i])) {
				yk = i;
				break;
			}
		}
		if (modelData.hasInterceptItem) {
			vec = vec.prefix(1.0);
		}
		double[] phi = calcPhi(vec, modelData.coefVector);
		if (yk < k1) {
			phi[yk] -= 1;
		}
		int featDim = modelData.coefVector.size() / k1;
		if (vec instanceof SparseVector) {
			int[] indices = ((SparseVector) vec).getIndices();
			double[] values = ((SparseVector) vec).getValues();
			int tmpIdx;
			for (int k = 0; k < k1; k++) {
				phi[k] = phi[k];
				tmpIdx = k * featDim;
				for (int i = 0; i < indices.length; ++i) {
					int idx = tmpIdx + indices[i];
					if (sparseGradient.containsKey(idx)) {
						sparseGradient.get(idx)[0] += values[i] * phi[k];
						sparseGradient.get(idx)[1] += 1.0;
					} else {
						sparseGradient.put(idx, new double[] {values[i] * phi[k], 1.0});
					}
				}
			}
		} else {
			double[] values = ((DenseVector) vec).getData();
			int tmpIdx;
			for (int k = 0; k < k1; k++) {
				phi[k] = phi[k];
				tmpIdx = k * featDim;
				for (int i = 0; i < featDim; i++) {
					int idx = tmpIdx + i;
					if (sparseGradient.containsKey(idx)) {
						sparseGradient.get(idx)[0] += values[i] * phi[k];
						sparseGradient.get(idx)[1] += 1.0;
					} else {
						sparseGradient.put(idx, new double[] {values[i] * phi[k], 1.0});
					}
				}
			}
		}
	}

	private double[] calcPhi(Vector vec, DenseVector coefficientVector) {
		double[] phi = new double[k1];
		int m = coefficientVector.size() / k1;
		double[] w = coefficientVector.getData();
		double expSum = 1;
		double eta;
		double[] x;
		if (vec instanceof DenseVector) {
			x = ((DenseVector) vec).getData();
			for (int k = 0; k < k1; k++) {
				eta = 0;
				for (int i = 0; i < m; i++) {
					eta += x[i] * w[k * m + i];
				}
				phi[k] = Math.exp(eta);
				expSum += phi[k];
			}
		} else {
			int[] indices = ((SparseVector) vec).getIndices();
			double[] values = ((SparseVector) vec).getValues();
			for (int k = 0; k < k1; k++) {
				eta = 0;
				for (int i = 0; i < indices.length; ++i) {
					eta += values[i] * w[k * m + indices[i]];
				}
				phi[k] = Math.exp(eta);
				expSum += phi[k];
			}
		}

		for (int k = 0; k < k1; k++) {
			phi[k] /= expSum;
		}
		return phi;
	}

	@Override
	public void deserializeModel(List <Row> modelRows) {
		modelData = new LinearModelDataConverter().load(modelRows);
		if (!optimMethod.equals(OptimMethod.SGD)) {
			this.nParam = new double[modelData.coefVector.size()];
		}
		if (optimMethod.equals(OptimMethod.ADAM) || optimMethod.equals(OptimMethod.FTRL)) {
			this.zParam = new double[modelData.coefVector.size()];
		}
		this.k1 = modelData.coefVectors.length;
	}
}