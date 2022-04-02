package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.OcsvmDetector;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

@NameCn("One-Class SVM异常检测")
public class OcsvmOutlierBatchOp extends BaseOutlierBatchOp <OcsvmOutlierBatchOp>
	implements OcsvmDetectorParams <OcsvmOutlierBatchOp> {

	public OcsvmOutlierBatchOp() {
		this(null);
	}

	public OcsvmOutlierBatchOp(Params params) {
		super(OcsvmDetector::new, params);
	}

}
