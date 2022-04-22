package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.KSigmaDetector;
import com.alibaba.alink.params.outlier.KSigmaDetectorParams;

@NameCn("KSigma异常检测")
@NameEn("KSigma Outlier")
/**
 * KSigma outlier for tuple data.
 */
public class KSigmaOutlierBatchOp extends BaseOutlierBatchOp <KSigmaOutlierBatchOp>
	implements KSigmaDetectorParams <KSigmaOutlierBatchOp> {

	public KSigmaOutlierBatchOp() {
		this(null);
	}

	public KSigmaOutlierBatchOp(Params params) {
		super(KSigmaDetector::new, params);
	}

}
