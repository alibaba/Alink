package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.EsdDetector;
import com.alibaba.alink.params.outlier.EsdDetectorParams;

@NameCn("ESD异常检测")
@NameEn("ESD Outlier")
/**
 * ESD outlier for data.
 */
public class EsdOutlierBatchOp extends BaseOutlierBatchOp <EsdOutlierBatchOp>
	implements EsdDetectorParams <EsdOutlierBatchOp> {

	public EsdOutlierBatchOp() {
		this(null);
	}

	public EsdOutlierBatchOp(Params params) {
		super(EsdDetector::new, params);
	}

}
