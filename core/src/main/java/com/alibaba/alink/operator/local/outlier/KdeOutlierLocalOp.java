package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.KdeDetector;
import com.alibaba.alink.params.outlier.KdeDetectorParams;

@NameCn("局部核密度估计异常检测")
@NameEn("KDE (Kernel Dense Estimate) Outlier Detection")
public class KdeOutlierLocalOp extends BaseOutlierLocalOp <KdeOutlierLocalOp>
	implements KdeDetectorParams <KdeOutlierLocalOp> {

	public KdeOutlierLocalOp() {
		this(null);
	}

	public KdeOutlierLocalOp(Params params) {
		super(KdeDetector::new, params);
	}
}
