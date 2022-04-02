package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.MadDetector;
import com.alibaba.alink.operator.common.outlier.MadDetectorParams;

public class MadOutlierBatchOp extends BaseOutlierBatchOp <MadOutlierBatchOp>
	implements MadDetectorParams <MadOutlierBatchOp> {
	public MadOutlierBatchOp() {
		this(null);
	}

	public MadOutlierBatchOp(Params params) {
		super(MadDetector::new, params);
	}

}
