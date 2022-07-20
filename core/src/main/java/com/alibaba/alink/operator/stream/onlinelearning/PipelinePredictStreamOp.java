package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.mapper.PipelineModelMapper;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.io.types.FlinkTypeConverter;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.onlinelearning.PipelineModelPredictParams;
import com.alibaba.alink.pipeline.PipelineModel;

/**
 *
 */
@NameCn("Pipeline在线预测")
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
			TableSchema outputSchema = PipelineModel.getOutSchema(pipelineModel, inputs[0].getSchema());
			StreamOperator <?> result = (pipelineModel.getModelStreamFilePath() == null) ?
				new InnerPredictStreamOp(
					pipelineModel.save())
					.setMLEnvironmentId(getMLEnvironmentId())
					.setNumThreads(getNumThreads())
					.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_NAMES, outputSchema.getFieldNames())
					.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_TYPES,
						FlinkTypeConverter.getTypeString(outputSchema.getFieldTypes())).linkFrom(inputs)
				: new InnerPredictStreamOp(
					pipelineModel.save())
					.setMLEnvironmentId(getMLEnvironmentId())
					.setNumThreads(getNumThreads())
					.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_NAMES, outputSchema.getFieldNames())
					.set(PipelineModelMapper.PIPELINE_TRANSFORM_OUT_COL_TYPES,
						FlinkTypeConverter.getTypeString(outputSchema.getFieldTypes()))
					.setModelStreamScanInterval(pipelineModel.getModelStreamScanInterval())
					.setModelStreamStartTime(pipelineModel.getModelStreamStartTime())
					.setModelStreamFilePath(pipelineModel.getModelStreamFilePath())
					.linkFrom(inputs);
			this.setOutputTable(
				DataStreamConversionUtil.toTable(getMLEnvironmentId(), result.getDataStream(), outputSchema));
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new AkIllegalDataException(ex.toString());
		}
		return this;
	}

	private static class InnerPredictStreamOp extends ModelMapStreamOp <InnerPredictStreamOp>
		implements MapperParams <InnerPredictStreamOp> {

		InnerPredictStreamOp() {
			super(PipelineModelMapper::new, new Params());
		}

		InnerPredictStreamOp(Params params) {
			super(PipelineModelMapper::new, params);
		}

		public InnerPredictStreamOp(BatchOperator <?> model) {
			this(model, new Params());
		}

		InnerPredictStreamOp(BatchOperator <?> model, Params params) {
			super(model, PipelineModelMapper::new, params);
		}
	}
}
