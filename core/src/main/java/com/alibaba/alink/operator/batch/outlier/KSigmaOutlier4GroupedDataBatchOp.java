package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.KSigmaDetector;
import com.alibaba.alink.params.outlier.KSigmaDetectorParams;

@NameCn("KSigma序列异常检测")
@NameEn("KSigma Series Outlier")
/**
 * KSigma outlier for series data.
 */
public class KSigmaOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <KSigmaOutlier4GroupedDataBatchOp>
	implements KSigmaDetectorParams <KSigmaOutlier4GroupedDataBatchOp> {

	public KSigmaOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public KSigmaOutlier4GroupedDataBatchOp(Params params) {
		super(KSigmaDetector::new, params);
	}

}

