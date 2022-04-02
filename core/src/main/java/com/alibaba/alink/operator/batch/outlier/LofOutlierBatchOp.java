package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.LofDetector;
import com.alibaba.alink.operator.common.outlier.LofDetectorParams;

@NameCn("局部异常因子异常检测")
@NameEn("LOF (Local Outlier Factor) Outlier Detection")
public class LofOutlierBatchOp extends BaseOutlierBatchOp <LofOutlierBatchOp>
	implements LofDetectorParams <LofOutlierBatchOp> {

	public LofOutlierBatchOp() {
		this(null);
	}

	public LofOutlierBatchOp(Params params) {
		super(LofDetector::new, params);
	}
}
