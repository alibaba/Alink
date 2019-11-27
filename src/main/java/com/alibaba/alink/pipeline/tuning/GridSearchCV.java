package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.tuning.HasNumFolds;
import com.alibaba.alink.pipeline.Pipeline;

public class GridSearchCV extends BaseGridSearch <GridSearchCV, GridSearchCVModel>
	implements HasNumFolds <GridSearchCV> {

	public GridSearchCV() {
		super();
	}

	@Override
	protected Tuple2<Pipeline, Report> findBest(BatchOperator in, PipelineCandidatesGrid candidates) {
		return findBestCV(in, getNumFolds(), candidates);
	}
}
