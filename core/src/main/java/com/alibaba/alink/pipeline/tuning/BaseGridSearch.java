package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.TransformerBase;

/**
 * BaseGridSearch.
 */
@NameCn("")
public abstract class BaseGridSearch<T extends BaseGridSearch <T, M>, M extends BaseTuningModel <M>>
	extends BaseTuning <T, M> {
	private static final long serialVersionUID = -548955496979331016L;
	private ParamGrid paramGrid;

	public BaseGridSearch() {
		super();
	}

	public ParamGrid getParamGrid() {
		return paramGrid;
	}

	public T setParamGrid(ParamGrid value) {
		this.paramGrid = value;
		return (T) this;
	}

	@Override
	protected Tuple2 <TransformerBase, Report> tuning(BatchOperator <?> in) {
		PipelineCandidatesGrid candidates = new PipelineCandidatesGrid(getEstimator(), getParamGrid());
		Tuple2 <Pipeline, Report> best = findBest(in, candidates);
		return Tuple2.of(best.f0.fit(in), best.f1);
	}

	@Override
	protected Tuple2 <TransformerBase, Report> tuning(LocalOperator <?> in) {
		PipelineCandidatesGrid candidates = new PipelineCandidatesGrid(getEstimator(), getParamGrid());
		Tuple2 <Pipeline, Report> best = findBest(in, candidates);
		return Tuple2.of(best.f0.fit(in), best.f1);
	}

	protected abstract Tuple2 <Pipeline, Report> findBest(BatchOperator <?> in, PipelineCandidatesGrid candidates);

	protected abstract Tuple2 <Pipeline, Report> findBest(LocalOperator <?> in, PipelineCandidatesGrid candidates);

}
