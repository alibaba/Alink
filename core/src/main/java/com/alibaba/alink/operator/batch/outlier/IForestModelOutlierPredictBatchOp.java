package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseModelOutlierPredictBatchOp;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector;

public class IForestModelOutlierPredictBatchOp
	extends BaseModelOutlierPredictBatchOp <IForestModelOutlierPredictBatchOp> {

	public IForestModelOutlierPredictBatchOp() {
		this(null);
	}

	public IForestModelOutlierPredictBatchOp(Params params) {
		super(IForestModelDetector::new, params);
	}
}
