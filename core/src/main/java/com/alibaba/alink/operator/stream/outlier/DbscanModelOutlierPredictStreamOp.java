package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierPredictStreamOp;
import com.alibaba.alink.operator.common.outlier.DbscanDetectorParams;
import com.alibaba.alink.operator.common.outlier.DbscanModelDetector;

public class DbscanModelOutlierPredictStreamOp
	extends BaseModelOutlierPredictStreamOp <DbscanModelOutlierPredictStreamOp>
	implements DbscanDetectorParams <DbscanModelOutlierPredictStreamOp> {

	public DbscanModelOutlierPredictStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public DbscanModelOutlierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, DbscanModelDetector::new, params);
	}
}
