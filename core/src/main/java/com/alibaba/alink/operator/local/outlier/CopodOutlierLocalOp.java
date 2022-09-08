package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.CopodDetector;
import com.alibaba.alink.params.outlier.CopodDetectorParams;

@NameCn("COP异常检测")
@NameEn("COP Outlier")
public class CopodOutlierLocalOp extends BaseOutlierLocalOp <CopodOutlierLocalOp>
	implements CopodDetectorParams <CopodOutlierLocalOp> {
	public CopodOutlierLocalOp() {
		this(null);
	}

	public CopodOutlierLocalOp(Params params) {
		super(CopodDetector::new, params);
	}
}