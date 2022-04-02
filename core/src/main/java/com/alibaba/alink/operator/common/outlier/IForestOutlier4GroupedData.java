package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;

public class IForestOutlier4GroupedData extends BaseOutlier4GroupedData <IForestOutlier4GroupedData> {

	public IForestOutlier4GroupedData() {
		this(null);
	}

	public IForestOutlier4GroupedData(Params params) {
		super(IForestDetector::new, params);
	}

}
