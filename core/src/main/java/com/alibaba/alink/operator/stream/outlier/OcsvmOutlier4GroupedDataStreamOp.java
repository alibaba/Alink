package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.OcsvmDetector;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

public class OcsvmOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <OcsvmOutlier4GroupedDataStreamOp>
	implements OcsvmDetectorParams <OcsvmOutlier4GroupedDataStreamOp> {

	public OcsvmOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public OcsvmOutlier4GroupedDataStreamOp(Params params) {
		super(OcsvmDetector::new, params);
	}

}
