package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.HbosDetector;
import com.alibaba.alink.operator.common.outlier.HbosDetectorParams;

public class HbosOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <HbosOutlier4GroupedDataStreamOp>
	implements HbosDetectorParams <HbosOutlier4GroupedDataStreamOp> {

	public HbosOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public HbosOutlier4GroupedDataStreamOp(Params params) {
		super(HbosDetector::new, params);
	}

}
