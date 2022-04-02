package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.CopodDetector;
import com.alibaba.alink.operator.common.outlier.CopodDetectorParams;

public class CopodOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <CopodOutlier4GroupedDataBatchOp>
	implements CopodDetectorParams <CopodOutlier4GroupedDataBatchOp> {

	public CopodOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public CopodOutlier4GroupedDataBatchOp(Params params) {
		super(CopodDetector::new, params);
	}
}

