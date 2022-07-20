package com.alibaba.alink.operator.stream.onlinelearning.kernel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.FmDataFormat;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LogitLoss;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.LossFunction;
import com.alibaba.alink.operator.common.fm.BaseFmTrainBatchOp.SquareLoss;
import com.alibaba.alink.operator.common.fm.FmModelData;
import com.alibaba.alink.operator.common.fm.FmModelDataConverter;
import com.alibaba.alink.params.onlinelearning.OnlineLearningTrainParams.OptimMethod;

import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.optim.FmOptimizer.calcY;

public class FmOnlineLearningKernel extends OnlineLearningKernel {
	public FmModelData modelData;
	private FmDataFormat nParam;
	private FmDataFormat zParam;
	final transient private LossFunction lossFunc;
	double[] regular;
	int[] dim;

	public FmOnlineLearningKernel(Params params,
								  boolean isClassification) {
		super(params);
		if (isClassification) {
			this.lossFunc = new LogitLoss();
		} else {
			double minTarget = 1.0e20;
			double maxTarget = -1.0e20;
			double d = maxTarget - minTarget;
			d = Math.max(d, 1.0);
			maxTarget = maxTarget + d * 0.2;
			minTarget = minTarget - d * 0.2;
			this.lossFunc = new SquareLoss(maxTarget, minTarget);
		}
	}

	@Override
	public int getVectorIdx(TableSchema inputSchema) {
		return modelData.featureColNames != null ? -1 : TableUtil.findColIndexWithAssertAndHint(inputSchema,
			modelData.vectorColName);
	}

	@Override
	public int getLabelIdx(TableSchema inputSchema) {
		return TableUtil.findColIndexWithAssertAndHint(inputSchema, modelData.labelColName);
	}

	@Override
	public int[] getFeatureIndices(TableSchema inputSchema) {
		return modelData.featureColNames != null ? TableUtil.findColIndices(inputSchema, modelData.featureColNames)
			: null;
	}

	@Override
	public Map <Integer, double[]> getGradient() {
		return sparseGradient;
	}

	@Override
	public void calcGradient(Vector vec, Object label) throws Exception {
		double y = label.equals(modelData.labelValues[0]) ? 1.0 : 0.0;

		Tuple2 <Double, double[]> yVx = calcY(vec, modelData.fmModel, dim);
		double dldy = lossFunc.dldy(y, yVx.f0);
		int[] indices;
		double[] values;
		if (vec instanceof SparseVector) {
			indices = ((SparseVector) vec).getIndices();
			values = ((SparseVector) vec).getValues();
		} else {
			indices = new int[vec.size()];
			for (int i = 0; i < vec.size(); ++i) {
				indices[i] = i;
			}
			values = ((DenseVector) vec).getData();
		}

		if (dim[0] > 0) {
			double biasGrad = dldy + regular[0] * modelData.fmModel.bias;
			if (sparseGradient.containsKey(-1)) {
				sparseGradient.get(-1)[1] += 1.0;
				sparseGradient.get(-1)[0] += biasGrad;
			} else {
				sparseGradient.put(-1, new double[] {biasGrad, 1.0});
			}
		}
		double[][] factors = modelData.fmModel.factors;
		for (int i = 0; i < indices.length; ++i) {
			int idx = indices[i];
			if (sparseGradient.containsKey(idx)) {
				double[] grad = sparseGradient.get(idx);
				grad[grad.length - 1] += 1.0;
				if (dim[1] > 0) {
					grad[dim[2]] += dldy * values[i] + regular[1] * factors[idx][dim[2]];
				}
				if (dim[2] > 0) {
					for (int j = 0; j < dim[2]; j++) {
						double vixi = values[i] * factors[idx][j];
						double d = values[i] * (yVx.f1[j] - vixi);
						grad[j] += dldy * d + regular[2] * factors[idx][j];
					}
				}
			} else {
				double[] grad = new double[dim[2] + dim[1] + 1];
				if (dim[1] > 0) {
					grad[dim[2]] = dldy * values[i] + regular[1] * factors[idx][dim[2]];
				}
				if (dim[2] > 0) {
					for (int j = 0; j < dim[2]; j++) {
						double vixi = values[i] * factors[idx][j];
						double d = values[i] * (yVx.f1[j] - vixi);
						grad[j] = dldy * d + regular[2] * factors[idx][j];
					}
				}
				grad[dim[2] + dim[1]] = 1.0;
				sparseGradient.put(idx, grad);
			}
		}
	}

	@Override
	public void updateModel(Object gradient) {
		Map <Integer, double[]> grad = (Map <Integer, double[]>) gradient;
		if (optimMethod.equals(OptimMethod.ADAM)) {
			beta1Power *= beta1;
			beta2Power *= beta2;
		}
		for (int idx : grad.keySet()) {
			// update fmModel
			if (idx == -1) {
				assert (grad.get(idx).length == 2);
				double biasGrad = grad.get(idx)[0] / grad.get(idx)[1];
				switch (optimMethod) {
					case FTRL:
						double sigma = (Math.sqrt(nParam.bias + biasGrad * biasGrad) - Math.sqrt(nParam.bias)) / alpha;
						zParam.bias += biasGrad - sigma * modelData.fmModel.bias;
						nParam.bias += biasGrad * biasGrad;
						if (Math.abs(zParam.bias) <= l1) {
							modelData.fmModel.bias = 0.0;
						} else {
							modelData.fmModel.bias = ((zParam.bias < 0 ? -1 : 1) * l1 - zParam.bias) / (
								(beta + Math.sqrt(
									nParam.bias)) / alpha + l2);
						}
						break;
					case ADAGRAD:
						nParam.bias += biasGrad * biasGrad;
						modelData.fmModel.bias -=
							learningRate * biasGrad / Math.sqrt(nParam.bias + EPS);
						break;
					case RMSprop:
						nParam.bias = gamma * nParam.bias + (1 - gamma) * biasGrad * biasGrad;
						modelData.fmModel.bias -=
							learningRate * biasGrad / Math.sqrt(nParam.bias + EPS);
						break;
					case ADAM:
						nParam.bias = beta1 * nParam.bias + (1 - beta1) * biasGrad;
						zParam.bias = beta2 * zParam.bias + (1 - beta2) * biasGrad * biasGrad;
						double nBar = nParam.bias / (1 - beta1Power);
						double zBar = zParam.bias / (1 - beta2Power);
						modelData.fmModel.bias -=
							learningRate * nBar / (Math.sqrt(zBar) + EPS);
						break;
					case SGD:
						modelData.fmModel.bias -= learningRate * biasGrad;
						break;
					case MOMENTUM:
						nParam.bias = gamma * nParam.bias + learningRate * biasGrad;
						modelData.fmModel.bias -= nParam.bias;
						break;
					default:
				}
			} else {
				double[] biasGradient = grad.get(idx);
				double[] factor = new double[biasGradient.length - 1];
				for (int i = 0; i < factor.length; ++i) {
					factor[i] = biasGradient[i] / biasGradient[factor.length];
				}
				if (dim[1] > 0) {
					updateModelVal(idx, dim[2], factor[dim[2]]);
				}
				if (dim[2] > 0) {
					for (int j = 0; j < dim[2]; j++) {
						updateModelVal(idx, j, factor[j]);
					}
				}
			}
		}
	}

	private void updateModelVal(int idx, int j, double gradVal) {
		switch (optimMethod) {
			case FTRL:
				double sigma = (Math.sqrt(nParam.factors[idx][j] + gradVal * gradVal)
					- Math.sqrt(nParam.factors[idx][j])) / alpha;
				zParam.factors[idx][j] += gradVal - sigma * modelData.fmModel.factors[idx][j];
				nParam.factors[idx][j] += gradVal * gradVal;
				if (Math.abs(zParam.factors[idx][j]) <= l1) {
					modelData.fmModel.factors[idx][j] = 0.0;
				} else {
					modelData.fmModel.factors[idx][j] =
						((zParam.factors[idx][j] < 0 ? -1 : 1) * l1 - zParam.factors[idx][j]) / ((beta + Math.sqrt(
							nParam.factors[idx][j])) / alpha + l2);
				}
			case ADAGRAD:
				nParam.factors[idx][j] += gradVal * gradVal;
				modelData.fmModel.factors[idx][j] -=
					learningRate * gradVal / Math.sqrt(nParam.factors[idx][j] + EPS);
				break;
			case RMSprop:
				nParam.factors[idx][j] = gamma * nParam.factors[idx][j] + (1 - gamma) * gradVal * gradVal;
				modelData.fmModel.factors[idx][j] -=
					learningRate * gradVal / Math.sqrt(nParam.factors[idx][j] + EPS);
				break;
			case ADAM:
				nParam.factors[idx][j] = beta1 * nParam.factors[idx][j] + (1 - beta1) * gradVal;
				zParam.factors[idx][j] = beta2 * zParam.factors[idx][j] + (1 - beta2) * gradVal * gradVal;
				double nBar = nParam.factors[idx][j] / (1 - beta1Power);
				double zBar = zParam.factors[idx][j] / (1 - beta2Power);
				modelData.fmModel.factors[idx][j] -=
					learningRate * nBar / (Math.sqrt(zBar) + EPS);
				break;
			case SGD:
				modelData.fmModel.factors[idx][j] -= learningRate * gradVal;
				break;
			case MOMENTUM:
				nParam.factors[idx][j] = gamma * nParam.factors[idx][j] + learningRate * gradVal;
				modelData.fmModel.factors[idx][j] -= nParam.factors[idx][j];
				break;
			default:
		}
	}

	@Override
	public List <Row> serializeModel() {
		RowCollector collector = new RowCollector();
		new FmModelDataConverter().save(modelData, collector);
		return collector.getRows();
	}

	@Override
	public void deserializeModel(List <Row> modelRows) {
		modelData = new FmModelDataConverter().load(modelRows);
		double[][] factors = modelData.fmModel.factors;
		if (!optimMethod.equals(OptimMethod.SGD)) {
			this.nParam = new FmDataFormat(factors.length, factors[0].length, modelData.dim, 0.0);
		}
		if (optimMethod.equals(OptimMethod.ADAM) || optimMethod.equals(OptimMethod.FTRL)) {
			this.zParam = new FmDataFormat(factors.length, factors[0].length, modelData.dim, 0.0);
		}
		this.dim = modelData.dim;
		this.regular = modelData.regular;
	}
}