package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.tuning.HasNumFolds;
import com.alibaba.alink.pipeline.Pipeline;

/**
 * random search cv.
 */
public class RandomSearchCV extends BaseRandomSearch <RandomSearchCV, RandomSearchCVModel>
	implements HasNumFolds <RandomSearchCV> {

	private static final long serialVersionUID = 2344498055854334406L;

	public RandomSearchCV() {
		super();
	}

	@Override
	protected Tuple2 <Pipeline, Report> findBest(BatchOperator <?> in, PipelineCandidatesRandom candidates) {
		return findBestCV(in, getNumFolds(), candidates);
	}
}
