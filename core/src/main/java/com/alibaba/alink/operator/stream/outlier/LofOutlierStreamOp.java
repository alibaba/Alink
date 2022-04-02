package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.LofDetector;
import com.alibaba.alink.operator.common.outlier.LofDetectorParams;

public class LofOutlierStreamOp extends BaseOutlierStreamOp <LofOutlierStreamOp>
	implements LofDetectorParams <LofOutlierStreamOp> {

	public LofOutlierStreamOp() {
		this(null);
	}

	public LofOutlierStreamOp(Params params) {
		super(LofDetector::new, params);
	}

}
