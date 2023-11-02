package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.DbscanBatchOp;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;
import com.alibaba.alink.params.clustering.DbscanParams;
import com.alibaba.alink.pipeline.EstimatorBase;

@NameCn("Dbscan训练")
public class Dbscan extends EstimatorBase <Dbscan, DbscanModel> implements
	DbscanParams <Dbscan>,
	ClusteringPredictParams <Dbscan> {

	private static final long serialVersionUID = 8233187816425264999L;

	public Dbscan() {
		super();
	}

	public Dbscan(Params params) {
		super(params);
	}

	/**
	 * Computes a DbscanModel.
	 *
	 * @param input input datasets.
	 * @return a DbscanModel
	 */
	@Override
	public DbscanModel fit(BatchOperator <?> input) {
		DbscanBatchOp dbscan = new DbscanBatchOp(getParams());
		dbscan.linkFrom(input);
		DbscanModel model = new DbscanModel(dbscan.getOutputTable(), dbscan.getSideOutput(0));
		model.getParams().merge(dbscan.getParams());
		return model;
	}
}
