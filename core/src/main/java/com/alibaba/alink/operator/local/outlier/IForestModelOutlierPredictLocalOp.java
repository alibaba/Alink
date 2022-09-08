package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector;

@NameCn("IForest模型异常检测预测")
public class IForestModelOutlierPredictLocalOp
	extends BaseModelOutlierPredictLocalOp <IForestModelOutlierPredictLocalOp> {

	public IForestModelOutlierPredictLocalOp() {
		this(null);
	}

	public IForestModelOutlierPredictLocalOp(Params params) {
		super(IForestModelDetector::new, params);
	}
}
