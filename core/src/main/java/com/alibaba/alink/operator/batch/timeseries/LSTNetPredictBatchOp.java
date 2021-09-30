package com.alibaba.alink.operator.batch.timeseries;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.tensorflow.TFTableModelPredictBatchOp;
import com.alibaba.alink.params.timeseries.LSTNetBatchPredictParams;

public class LSTNetPredictBatchOp extends BatchOperator <LSTNetPredictBatchOp>
	implements LSTNetBatchPredictParams <LSTNetPredictBatchOp> {

	@Override
	public LSTNetPredictBatchOp linkFrom(BatchOperator <?>... inputs) {
		TFTableModelPredictBatchOp predict = new TFTableModelPredictBatchOp()
			.setSelectedCols(new String[] {getSelectedCol()})
			.setSignatureDefKey("serving_default")
			.setInputSignatureDefs(new String[] {"tensor"})
			.setOutputSchemaStr(getPredictionCol() + " TENSOR_TYPES_FLOAT_TENSOR")
			.setOutputSignatureDefs(new String[] {"add"})
			.setReservedCols(getReservedCols())
			.setMLEnvironmentId(getMLEnvironmentId())
			.linkFrom(inputs);
		setOutputTable(predict.getOutputTable());
		return this;
	}
}
