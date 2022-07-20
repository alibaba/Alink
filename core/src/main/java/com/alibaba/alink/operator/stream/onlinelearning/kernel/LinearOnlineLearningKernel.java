package com.alibaba.alink.operator.stream.onlinelearning.kernel;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LinearModelDataConverter;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.unarylossfunc.LogLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SmoothHingeLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SquareLossFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.UnaryLossFunc;
import com.alibaba.alink.params.onlinelearning.OnlineLearningTrainParams.OptimMethod;

import java.util.List;
import java.util.Map;

public class LinearOnlineLearningKernel extends OnlineLearningKernel {
	protected LinearModelData modelData;
	protected double[] nParam;
	protected double[] zParam;
	private final UnaryLossFunc lossFunc;

	public LinearOnlineLearningKernel(Params params, LinearModelType type) {
		super(params);
		switch (type) {
			case LinearReg:
				lossFunc = new SquareLossFunc();
				break;
			case SVM:
				lossFunc = new SmoothHingeLossFunc();
				break;
			default:
				lossFunc = new LogLossFunc();
		}
	}

	@Override
	public int getVectorIdx(TableSchema inputSchema) {
		return modelData.featureNames != null ? -1 : TableUtil.findColIndexWithAssertAndHint(inputSchema,
			modelData.vectorColName);
	}

	@Override
	public int getLabelIdx(TableSchema inputSchema) {
		return TableUtil.findColIndexWithAssertAndHint(inputSchema, modelData.labelName);
	}

	@Override
	public int[] getFeatureIndices(TableSchema inputSchema) {
		return modelData.featureNames != null ? TableUtil.findColIndices(inputSchema, modelData.featureNames)
			: null;
	}

	@Override
	public Map <Integer, double[]> getGradient() {
		return sparseGradient;
	}

	@Override
	public void calcGradient(Vector vec, Object label) throws Exception {
		double y;
		if (modelData.labelValues.length == 2) {
			y = label.equals(modelData.labelValues[0]) ? 1.0 : -1.0;
		} else {
			y = ((Number) label).doubleValue();
		}
		if (modelData.hasInterceptItem) {
			vec = vec.prefix(1.0);
		}
		double eta = modelData.coefVector.dot(vec);
		double div = lossFunc.derivative(eta, y);

		if (vec instanceof DenseVector) {
			DenseVector denseVec = (DenseVector) vec;
			for (int i = 0; i < modelData.coefVector.size(); ++i) {
				if (sparseGradient.containsKey(i)) {
					sparseGradient.get(i)[0] += div * denseVec.getData()[i];
					sparseGradient.get(i)[1] += 1.0;
				} else {
					sparseGradient.put(i, new double[] {denseVec.getData()[i], 1.0});
				}
			}
		} else {
			SparseVector sparseVec = (SparseVector) vec;
			for (int i = 0; i < sparseVec.getIndices().length; ++i) {
				int idx = sparseVec.getIndices()[i];
				if (sparseGradient.containsKey(idx)) {
					sparseGradient.get(idx)[0] += div * sparseVec.getValues()[i];
					sparseGradient.get(idx)[1] += 1.0;
				} else {
					sparseGradient.put(idx, new double[] {div * sparseVec.getValues()[i], 1.0});
				}
			}
		}
	}

	@Override
	public void updateModel(Object gradient) {
		Map <Integer, double[]> grad = (Map <Integer, double[]>) gradient;
		int[] indices = new int[grad.size()];
		double[] values = new double[grad.size()];
		int iter = 0;
		for (Integer i : grad.keySet()) {
			indices[iter] = i;
			double[] gradVal = grad.get(i);
			values[iter++] = gradVal[0] / gradVal[1];
		}
		if (optimMethod.equals(OptimMethod.ADAM)) {
			beta1Power *= beta1;
			beta2Power *= beta2;
		}
		for (int i = 0; i < indices.length; ++i) {
			updateModelVal(indices[i], values[i]);
		}
	}

	private void updateModelVal(int idx, double gradVal) {
		switch (optimMethod) {
			case FTRL:
				double sigma = (Math.sqrt(nParam[idx] + gradVal * gradVal)
					- Math.sqrt(nParam[idx])) / alpha;
				zParam[idx] += gradVal - sigma * modelData.coefVector.get(idx);
				nParam[idx] += gradVal * gradVal;
				if (Math.abs(zParam[idx]) <= l1) {
					modelData.coefVector.set(idx, 0.0);
				} else {
					modelData.coefVector.set(idx,
						((zParam[idx] < 0 ? -1 : 1) * l1 - zParam[idx]) / ((beta + Math.sqrt(
							nParam[idx])) / alpha + l2));
				}
				break;
			case ADAGRAD:
				nParam[idx] += gradVal * gradVal;
				modelData.coefVector.set(idx, modelData.coefVector.get(idx)
					- learningRate * gradVal / Math.sqrt(nParam[idx] + EPS));
				break;
			case RMSprop:
				nParam[idx] = gamma * nParam[idx] + (1 - gamma) * gradVal * gradVal;
				modelData.coefVector.set(idx, modelData.coefVector.get(idx)
					- learningRate * gradVal / Math.sqrt(nParam[idx] + EPS));
				break;
			case ADAM:
				nParam[idx] = beta1 * nParam[idx] + (1 - beta1) * gradVal;
				zParam[idx] = beta2 * zParam[idx] + (1 - beta2) * gradVal * gradVal;
				double nBar = nParam[idx] / (1 - beta1Power);
				double zBar = zParam[idx] / (1 - beta2Power);
				modelData.coefVector.set(idx, modelData.coefVector.get(idx)
					- learningRate * nBar / (Math.sqrt(zBar) + EPS));
				break;
			case SGD:
				modelData.coefVector.set(idx, modelData.coefVector.get(idx) - learningRate * gradVal);
				break;
			case MOMENTUM:
				nParam[idx] = gamma * nParam[idx] + learningRate * gradVal;
				modelData.coefVector.set(idx, modelData.coefVector.get(idx) - nParam[idx]);
				break;
			default:
		}
	}

	@Override
	public List <Row> serializeModel() {
		RowCollector collector = new RowCollector();
		new LinearModelDataConverter().save(modelData, collector);
		return collector.getRows();
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
	}
}