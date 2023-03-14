package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;

@NameCn("SHEsd异常检测")
@NameEn("SHEsd Outlier")
public class SHEsdOutlierBatchOp extends BaseOutlierBatchOp <SHEsdOutlierBatchOp>
	implements SHEsdDetectorParams <SHEsdOutlierBatchOp> {

	public SHEsdOutlierBatchOp() {
		this(null);
	}

	public SHEsdOutlierBatchOp(Params params) {
		super(SHEsdDetector::new, params);
	}

}
