package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.EcodDetector;
import com.alibaba.alink.params.outlier.CopodDetectorParams;

@NameCn("ECOD序列异常检测")
@NameEn("ECOD Series Outlier")
public class EcodOutlier4GroupedDataLocalOp extends BaseOutlier4GroupedDataLocalOp <EcodOutlier4GroupedDataLocalOp>
	implements CopodDetectorParams <EcodOutlier4GroupedDataLocalOp> {

	public EcodOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public EcodOutlier4GroupedDataLocalOp(Params params) {
		super(EcodDetector::new, params);
	}
}