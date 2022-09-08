package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.EcodDetector;
import com.alibaba.alink.params.outlier.CopodDetectorParams;

@NameCn("ECOD异常检测")
@NameEn("ECOD Outlier")
public class EcodOutlierLocalOp extends BaseOutlierLocalOp <EcodOutlierLocalOp>
	implements CopodDetectorParams <EcodOutlierLocalOp> {
	public EcodOutlierLocalOp() {
		this(null);
	}

	public EcodOutlierLocalOp(Params params) {
		super(EcodDetector::new, params);
	}
}