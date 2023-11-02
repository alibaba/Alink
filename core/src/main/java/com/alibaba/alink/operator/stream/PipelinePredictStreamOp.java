package com.alibaba.alink.operator.stream;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.params.PipelinePredictParams;
import com.alibaba.alink.pipeline.PipelineModel;

/**
 *
 */
@NameCn("Pipeline在线预测")
@NameEn("Pipeline prediction")
public final class PipelinePredictStreamOp extends StreamOperator <PipelinePredictStreamOp>
	implements PipelinePredictParams <PipelinePredictStreamOp> {
	private PipelineModel pipelineModel;

	public PipelinePredictStreamOp(PipelineModel model) {
		this(model, new Params());
	}

	public PipelinePredictStreamOp(PipelineModel pipelineModel, Params params) {
		super(params);
		this.pipelineModel = pipelineModel;
	}

	public PipelinePredictStreamOp(Params params) {
		super(params);
	}

	@Override
	public PipelinePredictStreamOp linkFrom(StreamOperator <?>... inputs) {
		try {
			if (getParams().contains(PipelinePredictParams.MODEL_FILE_PATH)) {
				pipelineModel = PipelineModel.load(getModelFilePath())
					.setMLEnvironmentId(inputs[0].getMLEnvironmentId());
			}

			StreamOperator<?> result = pipelineModel.transform(inputs[0]);
			this.setOutput(DataStreamConversionUtil.toTable(getMLEnvironmentId(),
				result.getDataStream(), result.getSchema()));
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AkIllegalDataException(ex.toString());
		}
		return this;
	}
}
