package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.IForestDetector;
import com.alibaba.alink.operator.common.outlier.IForestDetectorParams;

public class IForestOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <IForestOutlier4GroupedDataBatchOp>
	implements IForestDetectorParams <IForestOutlier4GroupedDataBatchOp> {

	public IForestOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public IForestOutlier4GroupedDataBatchOp(Params params) {
		super(IForestDetector::new, params);
	}

}
