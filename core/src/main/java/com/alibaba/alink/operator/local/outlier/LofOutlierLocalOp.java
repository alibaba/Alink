package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.LofDetector;
import com.alibaba.alink.params.outlier.LofDetectorParams;

@NameCn("局部异常因子异常检测")
@NameEn("LOF (Local Outlier Factor) Outlier Detection")
public class LofOutlierLocalOp extends BaseOutlierLocalOp <LofOutlierLocalOp>
	implements LofDetectorParams <LofOutlierLocalOp> {

	public LofOutlierLocalOp() {
		this(null);
	}

	public LofOutlierLocalOp(Params params) {
		super(LofDetector::new, params);
	}
}
