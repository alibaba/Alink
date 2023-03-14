package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.shared.iter.HasNumIterDefaultAs10;
import com.alibaba.alink.params.tuning.BayesTuningParams;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.TransformerBase;

@NameCn("")
public abstract class BaseBayesSearch<T extends BaseBayesSearch <T, M>, M extends BaseTuningModel <M>>
	extends BaseTuning <T, M> implements
	HasNumIterDefaultAs10 <T>, BayesTuningParams <T> {

	private static final long serialVersionUID = -1703303076234747220L;
	private ParamDist paramDist;

	public BaseBayesSearch() {
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
		PipelineCandidatesBayes candidates = new PipelineCandidatesBayes(
			getEstimator(),
			getParamDist(),
			System.currentTimeMillis(),
			getBayesNumStartup(),
			getNumIter(),
			getBayesNumCandidates(),
			getBayesTpeLinearForgetting(),
			getBayesStrategy(),
			getTuningEvaluator().isLargerBetter());
		Tuple2 <Pipeline, Report> best = findBest(in, candidates);
		return Tuple2.of(best.f0.fit(in), best.f1);
	}

	@Override
	protected Tuple2 <TransformerBase, Report> tuning(LocalOperator <?> in) {
		PipelineCandidatesBayes candidates = new PipelineCandidatesBayes(
			getEstimator(),
			getParamDist(),
			System.currentTimeMillis(),
			getBayesNumStartup(),
			getNumIter(),
			getBayesNumCandidates(),
			getBayesTpeLinearForgetting(),
			getBayesStrategy(),
			getTuningEvaluator().isLargerBetter());
		Tuple2 <Pipeline, Report> best = findBest(in, candidates);
		return Tuple2.of(best.f0.fit(in), best.f1);
	}

	protected abstract Tuple2 <Pipeline, Report> findBest(BatchOperator <?> in,
														  PipelineCandidatesBayes candidates);

	protected abstract Tuple2 <Pipeline, Report> findBest(LocalOperator <?> in,
														  PipelineCandidatesBayes candidates);

}
