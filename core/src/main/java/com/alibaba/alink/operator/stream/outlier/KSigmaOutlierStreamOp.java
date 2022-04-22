package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.KSigmaDetector;
import com.alibaba.alink.params.outlier.KSigmaDetectorParams;

@NameCn("KSigma异常检测")
@NameEn("KSigma Outlier")
/**
 * KSigma outlier for tuple data.
 */
public class KSigmaOutlierStreamOp extends BaseOutlierStreamOp <KSigmaOutlierStreamOp>
	implements KSigmaDetectorParams <KSigmaOutlierStreamOp> {

	public KSigmaOutlierStreamOp() {
		this(null);
	}

	public KSigmaOutlierStreamOp(Params params) {
		super(KSigmaDetector::new, params);
	}

}
