package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.shared.iter.HasNumIterDefaultAs10;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.TransformerBase;

public abstract class BaseRandomSearch<T extends BaseRandomSearch <T, M>, M extends BaseTuningModel <M>>
	extends BaseTuning <T, M> implements
	HasNumIterDefaultAs10 <T> {

	private static final long serialVersionUID = 6628318837991338931L;
	private ParamDist paramDist;

	public BaseRandomSearch() {
		super();
	}

	public ParamDist getParamDist() {
		return paramDist;
	}

	public T setParamDist(ParamDist value) {
		this.paramDist = value;
		return (T) this;
	}

	@Override
	protected Tuple2 <TransformerBase, Report> tuning(BatchOperator <?> in) {
		PipelineCandidatesRandom candidates = new PipelineCandidatesRandom(
			getEstimator(),
			getParamDist(),
			System.currentTimeMillis(),
			getNumIter());
		Tuple2 <Pipeline, Report> best = findBest(in, candidates);
		return Tuple2.of(best.f0.fit(in), best.f1);
	}

	protected abstract Tuple2 <Pipeline, Report> findBest(BatchOperator <?> in, PipelineCandidatesRandom candidates);

}
