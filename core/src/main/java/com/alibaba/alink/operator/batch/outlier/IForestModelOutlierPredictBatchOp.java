package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierPredictBatchOp;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector;

@NameCn("IForest模型异常检测预测")
@NameEn("IForest Model Outlier Predict")
public class IForestModelOutlierPredictBatchOp
	extends BaseModelOutlierPredictBatchOp <IForestModelOutlierPredictBatchOp> {

	public IForestModelOutlierPredictBatchOp() {
		this(null);
	}

	public IForestModelOutlierPredictBatchOp(Params params) {
		super(IForestModelDetector::new, params);
	}
}
