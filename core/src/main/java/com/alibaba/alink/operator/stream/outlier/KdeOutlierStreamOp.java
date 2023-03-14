package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.KdeDetector;
import com.alibaba.alink.params.outlier.KdeDetectorParams;

@NameCn("局部核密度估计异常检测")
@NameEn("LDE (Local Dense Estimate) Outlier Detection")
public class KdeOutlierStreamOp extends BaseOutlierStreamOp <KdeOutlierStreamOp>
	implements KdeDetectorParams <KdeOutlierStreamOp> {

	public KdeOutlierStreamOp() {
		this(null);
	}

	public KdeOutlierStreamOp(Params params) {
		super(KdeDetector::new, params);
	}
}
