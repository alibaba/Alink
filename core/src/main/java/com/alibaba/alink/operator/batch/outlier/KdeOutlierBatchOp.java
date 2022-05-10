package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.KdeDetector;
import com.alibaba.alink.params.outlier.KdeDetectorParams;

@NameCn("局部核密度估计异常检测")
@NameEn("KDE (Kernel Dense Estimate) Outlier Detection")
public class KdeOutlierBatchOp extends BaseOutlierBatchOp <KdeOutlierBatchOp>
	implements KdeDetectorParams <KdeOutlierBatchOp> {

	public KdeOutlierBatchOp() {
		this(null);
	}

	public KdeOutlierBatchOp(Params params) {
		super(KdeDetector::new, params);
	}
}
