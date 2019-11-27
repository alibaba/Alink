package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.tuning.HasTrainRatio;
import com.alibaba.alink.pipeline.Pipeline;

public class GridSearchTVSplit extends BaseGridSearch <GridSearchTVSplit, GridSearchTVSplitModel>
	implements HasTrainRatio <GridSearchTVSplit> {

	public GridSearchTVSplit() {
		super();
	}

	@Override
	protected Tuple2<Pipeline, Report> findBest(BatchOperator in, PipelineCandidatesGrid candidates) {
		return findBestTVSplit(in, getTrainRatio(), candidates);
	}
}
