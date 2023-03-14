package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.tuning.HasNumFolds;
import com.alibaba.alink.pipeline.Pipeline;

@NameCn("Bayes搜索CV")
public class BayesSearchCV extends BaseBayesSearch <BayesSearchCV, BayesSearchCVModel>
	implements HasNumFolds <BayesSearchCV> {

	private static final long serialVersionUID = -496513145782913263L;

	public BayesSearchCV() {
		super();
	}

	@Override
	protected Tuple2 <Pipeline, Report> findBest(BatchOperator <?> in, PipelineCandidatesBayes candidates) {
		return findBestCV(in, getNumFolds(), candidates);
	}

	@Override
	protected Tuple2 <Pipeline, Report> findBest(LocalOperator <?> in, PipelineCandidatesBayes candidates) {
		return findBestCV(in, getNumFolds(), candidates);
	}
}
