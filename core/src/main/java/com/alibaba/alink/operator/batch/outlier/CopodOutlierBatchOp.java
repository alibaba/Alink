package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.CopodDetector;
import com.alibaba.alink.params.outlier.CopodDetectorParams;

@NameCn("COP异常检测")
@NameEn("COP Outlier")
public class CopodOutlierBatchOp extends BaseOutlierBatchOp <CopodOutlierBatchOp>
	implements CopodDetectorParams <CopodOutlierBatchOp> {
	public CopodOutlierBatchOp() {
		this(null);
	}

	public CopodOutlierBatchOp(Params params) {
		super(CopodDetector::new, params);
	}
}