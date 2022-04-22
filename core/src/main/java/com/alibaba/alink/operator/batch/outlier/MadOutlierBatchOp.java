package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.MadDetector;
import com.alibaba.alink.params.outlier.MadDetectorParams;

@NameCn("Mad异常检测")
@NameEn("Mad Outlier")
/**
 * Mad outlier for data.
 */
public class MadOutlierBatchOp extends BaseOutlierBatchOp <MadOutlierBatchOp>
	implements MadDetectorParams <MadOutlierBatchOp> {
	public MadOutlierBatchOp() {
		this(null);
	}

	public MadOutlierBatchOp(Params params) {
		super(MadDetector::new, params);
	}

}
