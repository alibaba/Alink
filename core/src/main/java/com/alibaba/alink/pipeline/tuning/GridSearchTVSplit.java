package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.tuning.HasTrainRatio;
import com.alibaba.alink.pipeline.Pipeline;

/**
 * Grid search implemented by Train-Validation.
 *
 * Grid search is an approach to parameter tuning that will methodically build and evaluate a model for each combination
 * of algorithm parameters specified in a grid.
 */
@NameCn("网格搜索TV")
public class GridSearchTVSplit extends BaseGridSearch <GridSearchTVSplit, GridSearchTVSplitModel>
	implements HasTrainRatio <GridSearchTVSplit> {

	private static final long serialVersionUID = -1128160766782268679L;

	public GridSearchTVSplit() {
		super();
	}

	@Override
	protected Tuple2 <Pipeline, Report> findBest(BatchOperator <?> in, PipelineCandidatesGrid candidates) {
		return findBestTVSplit(in, getTrainRatio(), candidates);
	}
}
