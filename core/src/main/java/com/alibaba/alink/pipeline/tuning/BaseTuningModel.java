package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.operator.stream.StreamOperator;

public abstract class BaseTuningModel<M extends BaseTuningModel <M>> extends ModelBase<M> {

	private final TransformerBase transformer;
	private final Report report;

	public BaseTuningModel(TransformerBase transformer, Report report) {
		super(null);
		this.transformer = transformer;
		this.report = report;
	}

	public Report getReport() {
		return report;
	}

	@Override
	public BatchOperator transform(BatchOperator input) {
		return this.transformer.transform(input);
	}

	@Override
	public StreamOperator transform(StreamOperator input) {
		return this.transformer.transform(input);
	}
}
