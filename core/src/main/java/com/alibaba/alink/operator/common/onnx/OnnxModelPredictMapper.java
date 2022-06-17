package com.alibaba.alink.operator.common.onnx;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.dl.plugin.DLPredictServiceMapper;
import com.alibaba.alink.common.dl.plugin.DLPredictorService;
import com.alibaba.alink.common.dl.plugin.OnnxPredictorClassLoaderFactory;
import com.alibaba.alink.params.dl.HasIntraOpParallelism;
import com.alibaba.alink.params.onnx.OnnxModelPredictParams;

public class OnnxModelPredictMapper extends DLPredictServiceMapper <OnnxPredictorClassLoaderFactory> {

	protected DLPredictorService predictor;
	private String[] inputNames;
	private String[] outputNames;

	public OnnxModelPredictMapper(TableSchema dataSchema, Params params) {
		this(dataSchema, params, new OnnxPredictorClassLoaderFactory());
	}

	public OnnxModelPredictMapper(TableSchema dataSchema, Params params, OnnxPredictorClassLoaderFactory factory) {
		super(dataSchema, params, factory, true);

		inputNames = params.get(OnnxModelPredictParams.INPUT_NAMES);
		if (null == inputNames) {
			inputNames = inputCols;
		}

		outputNames = params.get(OnnxModelPredictParams.OUTPUT_NAMES);
		if (null == outputNames) {
			outputNames = outputCols;
		}
	}

	@Override
	protected PredictorConfig getPredictorConfig() {
		PredictorConfig config = new PredictorConfig();
		config.factory = factory;
		Integer intraOpParallelism = params.contains(HasIntraOpParallelism.INTRA_OP_PARALLELISM)
			? params.get(HasIntraOpParallelism.INTRA_OP_PARALLELISM)
			: HasIntraOpParallelism.INTRA_OP_PARALLELISM.getDefaultValue();
		config.modelPath = localModelPath;
		config.inputNames = inputNames;
		config.outputNames = outputNames;
		config.outputTypeClasses = outputColTypeClasses;
		config.intraOpNumThreads = intraOpParallelism;
		config.threadMode = false;
		return config;
	}
}
