package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.HbosDetector;
import com.alibaba.alink.operator.common.outlier.HbosDetectorParams;

public class HbosOutlierBatchOp extends BaseOutlierBatchOp <HbosOutlierBatchOp>
	implements HbosDetectorParams <HbosOutlierBatchOp> {

	public HbosOutlierBatchOp() {
		this(null);
	}

	public HbosOutlierBatchOp(Params params) {
		super(HbosDetector::new, params);
	}

}
