package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.KSigmaDetector;
import com.alibaba.alink.operator.common.outlier.KSigmaDetectorParams;

@NameCn("KSigma序列异常检测")
@NameEn("KSigma Series Outlier")
/**
 * KSigma outlier for series data.
 */
public class KSigmaOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <KSigmaOutlier4GroupedDataStreamOp>
	implements KSigmaDetectorParams <KSigmaOutlier4GroupedDataStreamOp> {

	public KSigmaOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public KSigmaOutlier4GroupedDataStreamOp(Params params) {
		super(KSigmaDetector::new, params);
	}

}
