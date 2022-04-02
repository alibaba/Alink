package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.CopodDetectorParams;
import com.alibaba.alink.operator.common.outlier.EcodDetector;

public class EcodOutlierStreamOp extends BaseOutlierStreamOp <EcodOutlierStreamOp>
	implements CopodDetectorParams <EcodOutlierStreamOp> {

	public EcodOutlierStreamOp() {this(null);}

	public EcodOutlierStreamOp(Params params) {
		super(EcodDetector::new, params);
	}
}
