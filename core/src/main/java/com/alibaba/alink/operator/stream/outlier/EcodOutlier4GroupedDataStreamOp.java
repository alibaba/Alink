package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.params.outlier.CopodDetectorParams;
import com.alibaba.alink.operator.common.outlier.EcodDetector;

@NameCn("ECOD序列异常检测")
@NameEn("ECOD Series Outlier")
public class EcodOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <EcodOutlier4GroupedDataStreamOp>
	implements CopodDetectorParams <EcodOutlier4GroupedDataStreamOp> {

	public EcodOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public EcodOutlier4GroupedDataStreamOp(Params params) {
		super(EcodDetector::new, params);
	}
}

