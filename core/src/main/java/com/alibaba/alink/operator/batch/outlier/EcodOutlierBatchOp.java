package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.params.outlier.CopodDetectorParams;
import com.alibaba.alink.operator.common.outlier.EcodDetector;

@NameCn("ECOD异常检测")
@NameEn("ECOD Outlier")
public class EcodOutlierBatchOp extends BaseOutlierBatchOp <EcodOutlierBatchOp>
	implements CopodDetectorParams <EcodOutlierBatchOp> {
	public EcodOutlierBatchOp() {
		this(null);
	}

	public EcodOutlierBatchOp(Params params) {
		super(EcodDetector::new, params);
	}
}