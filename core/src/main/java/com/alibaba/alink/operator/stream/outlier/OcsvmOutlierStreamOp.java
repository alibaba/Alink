package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.OcsvmDetector;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

public class OcsvmOutlierStreamOp extends BaseOutlierStreamOp <OcsvmOutlierStreamOp>
	implements OcsvmDetectorParams <OcsvmOutlierStreamOp> {

	public OcsvmOutlierStreamOp() {
		this(null);
	}

	public OcsvmOutlierStreamOp(Params params) {
		super(OcsvmDetector::new, params);
	}

}
