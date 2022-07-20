package com.alibaba.alink.operator.stream.onlinelearning.kernel;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.params.onlinelearning.OnlineLearningTrainParams;
import com.alibaba.alink.params.onlinelearning.OnlineLearningTrainParams.OptimMethod;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class OnlineLearningKernel implements Serializable {

	protected final double alpha;
	protected final double beta;
	protected final double l1;
	protected final double l2;
	protected final static double EPS = 1.0e-8;
	protected final OptimMethod optimMethod;
	protected double learningRate;
	protected double beta1Power = 1.0;
	protected double beta2Power = 1.0;
	protected final double gamma;
	protected double beta1;
	protected double beta2;
	protected final Map <Integer, double[]> sparseGradient = new HashMap <>();

	public OnlineLearningKernel(Params params) {
		alpha = params.get(OnlineLearningTrainParams.ALPHA);
		beta = params.get(OnlineLearningTrainParams.BETA);
		l1 = params.get(OnlineLearningTrainParams.L_1);
		l2 = params.get(OnlineLearningTrainParams.L_2);
		this.optimMethod = params.get(OnlineLearningTrainParams.OPTIM_METHOD);
		if (params.get(OnlineLearningTrainParams.LEARNING_RATE) == null) {
			switch (optimMethod) {
				case SGD:
					this.learningRate = 0.0005;
					break;
				case ADAM:
				case RMSprop:
				case MOMENTUM:
					this.learningRate = 0.001;
					break;
				case ADAGRAD:
					this.learningRate = 0.01;
				default:
			}
		} else {
			this.learningRate = params.get(OnlineLearningTrainParams.LEARNING_RATE);
		}
		this.gamma = params.get(OnlineLearningTrainParams.GAMMA);
		this.beta1 = params.get(OnlineLearningTrainParams.BETA_1);
		this.beta2 = params.get(OnlineLearningTrainParams.BETA_2);
	}

	public abstract int getVectorIdx(TableSchema inputSchema);

	public abstract int getLabelIdx(TableSchema inputSchema);

	public abstract int[] getFeatureIndices(TableSchema inputSchema);

	public abstract Map <Integer, double[]> getGradient();

	public abstract void calcGradient(Vector vec, Object label) throws Exception;

	public abstract void updateModel(Object gradient);

	public abstract List <Row> serializeModel();

	public abstract void deserializeModel(List <Row> modelRows);
}
