package com.alibaba.alink.operator.batch.timeseries;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelTrainBatchOp;
import com.alibaba.alink.params.timeseries.LSTNetTrainParams;

import java.util.HashMap;
import java.util.Map;

public class LSTNetTrainBatchOp extends BatchOperator <LSTNetTrainBatchOp>
	implements LSTNetTrainParams <LSTNetTrainBatchOp> {

	@Override
	public LSTNetTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> input = checkAndGetFirst(inputs);
		BatchOperator <?> preprocessed = new LSTNetPreProcessBatchOp(getParams().clone())
			.setOutputCols("tensor", "y")
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(input);

		Map <String, Object> modelConfig = new HashMap <>();
		modelConfig.put("window", getWindow());
		modelConfig.put("horizon", getHorizon());

		Map <String, String> userParams = new HashMap <>();
		userParams.put("tensorCol", "tensor");
		userParams.put("labelCol", "y");
		userParams.put("batch_size", String.valueOf(getBatchSize()));
		userParams.put("num_epochs", String.valueOf(getNumEpochs()));
		userParams.put("model_config", JsonConverter.toJson(modelConfig));

		TFTableModelTrainBatchOp tfTableModelTrainBatchOp = new TFTableModelTrainBatchOp(getParams().clone())
			.setSelectedCols("tensor", "y")
			.setUserFiles(new String[] {"res:///tf_algos/lstnet_entry.py"})
			.setMainScriptFile("res:///tf_algos/lstnet_entry.py")
			.setUserParams(JsonConverter.toJson(userParams))
			.linkFrom(preprocessed)
			.setMLEnvironmentId(getMLEnvironmentId());
		setOutputTable(tfTableModelTrainBatchOp.getOutputTable());
		return this;
	}
}
