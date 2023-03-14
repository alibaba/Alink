package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.dbscan.DbscanModelMapper;
import com.alibaba.alink.operator.common.feature.CrossFeatureModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.ClusteringPredictParams;

/**
 * @author guotao.gt
 */

@NameCn("DBSCAN预测")
@NameEn("DBSCAN Prediction")
public final class DbscanPredictStreamOp extends ModelMapStreamOp <DbscanPredictStreamOp>
	implements ClusteringPredictParams <DbscanPredictStreamOp> {

	private static final long serialVersionUID = -4804905051790558990L;

	public DbscanPredictStreamOp() {
		super(DbscanModelMapper::new, new Params());
	}

	public DbscanPredictStreamOp(Params params) {
		super(DbscanModelMapper::new, params);
	}

	public DbscanPredictStreamOp(BatchOperator model) {
		super(model, DbscanModelMapper::new, new Params());
	}

	public DbscanPredictStreamOp(BatchOperator model, Params params) {
		super(model, DbscanModelMapper::new, params);
	}
}
