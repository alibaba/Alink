package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.KSigmaDetector;
import com.alibaba.alink.params.outlier.KSigmaDetectorParams;

@NameCn("KSigma异常检测")
@NameEn("KSigma Outlier")
/**
 * KSigma outlier for tuple data.
 */
public class KSigmaOutlierLocalOp extends BaseOutlierLocalOp <KSigmaOutlierLocalOp>
	implements KSigmaDetectorParams <KSigmaOutlierLocalOp> {

	public KSigmaOutlierLocalOp() {
		this(null);
	}

	public KSigmaOutlierLocalOp(Params params) {
		super(KSigmaDetector::new, params);
	}

}
