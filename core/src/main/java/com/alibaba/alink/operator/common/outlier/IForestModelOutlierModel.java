package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;

public class IForestModelOutlierModel extends BaseModelOutlierModel <IForestModelOutlierModel> {

	public IForestModelOutlierModel() {
		this(null);
	}

	public IForestModelOutlierModel(Params params) {
		super(IForestModelDetector::new, params);
	}

}
