package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierPredictStreamOp;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector;

public class IForestModelOutlierPredictStreamOp
	extends BaseModelOutlierPredictStreamOp <IForestModelOutlierPredictStreamOp> {

	public IForestModelOutlierPredictStreamOp(BatchOperator <?> model) {
		this(model, null);
	}

	public IForestModelOutlierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, IForestModelDetector::new, params);
	}
}
