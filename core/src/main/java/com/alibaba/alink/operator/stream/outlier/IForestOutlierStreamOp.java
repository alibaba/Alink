package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.operator.common.outlier.IForestDetectorParams;

public class IForestOutlierStreamOp extends BaseOutlierStreamOp <IForestOutlierStreamOp>
	implements IForestDetectorParams <IForestOutlierStreamOp> {

	public IForestOutlierStreamOp() {
		this(null);
	}

	public IForestOutlierStreamOp(Params params) {
		super(IForestDetector::new, params);
	}

}
