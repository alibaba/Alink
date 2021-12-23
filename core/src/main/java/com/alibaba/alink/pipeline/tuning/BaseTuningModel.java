package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.util.Preconditions;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.TransformerBase;

public abstract class BaseTuningModel<M extends BaseTuningModel <M>> extends ModelBase <M> {

	private static final long serialVersionUID = -1991753518931044555L;
	private final TransformerBase transformer;

	public BaseTuningModel(TransformerBase transformer) {
		super(null);
		this.transformer = transformer;
	}

	@Override
	public BatchOperator <?> transform(BatchOperator <?> input) {
		return postProcessTransformResult(this.transformer.transform(input));
	}

	@Override
	public StreamOperator <?> transform(StreamOperator <?> input) {
		return this.transformer.transform(input);
	}

	public PipelineModel getBestPipelineModel() {
		Preconditions.checkArgument(transformer instanceof PipelineModel, "Best model should be a pipeline model.");

		return (PipelineModel) transformer;
	}
}
