package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.onlinelearning.PipelineModelPredictParams;
import com.alibaba.alink.pipeline.PipelineModel;

/**
 *
 */
@NameCn("Pipeline在线预测")
@NameEn("Pipeline prediction")
public final class PipelinePredictStreamOp extends StreamOperator <PipelinePredictStreamOp>
	implements PipelineModelPredictParams <PipelinePredictStreamOp> {
	private final PipelineModel pipelineModel;

	public PipelinePredictStreamOp(PipelineModel model) {
		this(model, new Params());
	}

	public PipelinePredictStreamOp(PipelineModel pipelineModel, Params params) {
		super(params);
		this.pipelineModel = pipelineModel;
	}

	public PipelinePredictStreamOp(String modelPath) {
		this(modelPath, new Params());
	}

	public PipelinePredictStreamOp(String modelPath, Params params) {
		super(params);
		this.pipelineModel = PipelineModel.load(modelPath);
	}

	@Override
	public PipelinePredictStreamOp linkFrom(StreamOperator <?>... inputs) {
		try {
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
