package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.LofDetector;
import com.alibaba.alink.params.outlier.LofDetectorParams;

@NameCn("局部异常因子异常检测")
@NameEn("LOF (Local Outlier Factor) Outlier Detection")
public class LofOutlierStreamOp extends BaseOutlierStreamOp <LofOutlierStreamOp>
	implements LofDetectorParams <LofOutlierStreamOp> {

	public LofOutlierStreamOp() {
		this(null);
	}

	public LofOutlierStreamOp(Params params) {
		super(LofDetector::new, params);
	}

}
