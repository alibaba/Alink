package com.alibaba.alink.operator.batch;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.params.PipelinePredictParams;
import com.alibaba.alink.pipeline.PipelineModel;

/**
 * Pipeline prediction.
 */

@NameCn("Pipeline 预测")
@NameEn("Pipeline prediction")
public final class PipelinePredictBatchOp extends BatchOperator <PipelinePredictBatchOp>
	implements PipelinePredictParams <PipelinePredictBatchOp> {

	public PipelinePredictBatchOp() {
		super(new Params());
	}

	public PipelinePredictBatchOp(Params params) {
		super(params);
	}

	@Override
	public PipelinePredictBatchOp linkFrom(BatchOperator <?>... inputs) {
		try {
			BatchOperator<?> data = checkAndGetFirst(inputs);
			final PipelineModel pipelineModel = PipelineModel.load(getModelFilePath())
				.setMLEnvironmentId(data.getMLEnvironmentId());
			BatchOperator<?> result = pipelineModel.transform(data);
			this.setOutput(DataSetConversionUtil.toTable(data.getMLEnvironmentId(),
				result.getDataSet(), result.getSchema()));
			return this;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AkIllegalDataException(ex.toString());
		}
	}
}
