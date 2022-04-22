package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.SosDetector;
import com.alibaba.alink.params.outlier.SosDetectorParams;

public class SosOutlierStreamOp extends BaseOutlierStreamOp <SosOutlierStreamOp>
	implements SosDetectorParams <SosOutlierStreamOp> {

	public SosOutlierStreamOp() {
		this(null);
	}

	public SosOutlierStreamOp(Params params) {
		super(SosDetector::new, params);
	}

}
