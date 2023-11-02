package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.SHEsdDetector;
import com.alibaba.alink.operator.common.outlier.SHEsdDetectorParams;

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
