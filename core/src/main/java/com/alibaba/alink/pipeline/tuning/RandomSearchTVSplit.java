package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.tuning.HasTrainRatio;
import com.alibaba.alink.pipeline.Pipeline;

/**
 * random search tv split.
 */
public class RandomSearchTVSplit extends BaseRandomSearch <RandomSearchTVSplit, RandomSearchTVSplitModel>
	implements HasTrainRatio <RandomSearchTVSplit> {

	private static final long serialVersionUID = 814250767752009366L;

	public RandomSearchTVSplit() {
		super();
	}

	@Override
	protected Tuple2 <Pipeline, Report> findBest(BatchOperator <?> in, PipelineCandidatesRandom candidates) {
		return findBestTVSplit(in, getTrainRatio(), candidates);
	}
}
