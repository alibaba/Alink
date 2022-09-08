package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.KSigmaDetector;
import com.alibaba.alink.params.outlier.KSigmaDetectorParams;

@NameCn("KSigma序列异常检测")
@NameEn("KSigma Series Outlier")
/**
 * KSigma outlier for series data.
 */
public class KSigmaOutlier4GroupedDataLocalOp extends BaseOutlier4GroupedDataLocalOp <KSigmaOutlier4GroupedDataLocalOp>
	implements KSigmaDetectorParams <KSigmaOutlier4GroupedDataLocalOp> {

	public KSigmaOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public KSigmaOutlier4GroupedDataLocalOp(Params params) {
		super(KSigmaDetector::new, params);
	}

}

