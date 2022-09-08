package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.CopodDetector;
import com.alibaba.alink.params.outlier.CopodDetectorParams;

@NameCn("COP序列异常检测")
@NameEn("COP Series Outlier")
public class CopodOutlier4GroupedDataLocalOp extends BaseOutlier4GroupedDataLocalOp <CopodOutlier4GroupedDataLocalOp>
	implements CopodDetectorParams <CopodOutlier4GroupedDataLocalOp> {

	public CopodOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public CopodOutlier4GroupedDataLocalOp(Params params) {
		super(CopodDetector::new, params);
	}
}

