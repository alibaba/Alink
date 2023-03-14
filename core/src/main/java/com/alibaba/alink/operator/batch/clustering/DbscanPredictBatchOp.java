package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanModelMapper;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;

/**
 * input parameters: -# predResultColName: required
 */

@NameCn("DBSCAN预测")
@NameEn("DBSCAN Prediction")
public final class DbscanPredictBatchOp extends ModelMapBatchOp <DbscanPredictBatchOp>
	implements ClusteringPredictParams <DbscanPredictBatchOp> {

	private static final long serialVersionUID = -7841302650523879193L;

	public DbscanPredictBatchOp() {
		this(new Params());
	}

	public DbscanPredictBatchOp(Params params) {
		super(DbscanModelMapper::new, params);
	}
}
