package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.MadDetector;
import com.alibaba.alink.params.outlier.MadDetectorParams;

@NameCn("Mad异常检测")
@NameEn("Mad Outlier")
/**
 * Mad outlier for data.
 */
public class MadOutlierLocalhOp extends BaseOutlierLocalOp <MadOutlierLocalhOp>
	implements MadDetectorParams <MadOutlierLocalhOp> {
	public MadOutlierLocalhOp() {
		this(null);
	}

	public MadOutlierLocalhOp(Params params) {
		super(MadDetector::new, params);
	}

}
