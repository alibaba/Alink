package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.SosDetector;
import com.alibaba.alink.params.outlier.SosDetectorParams;

@NameCn("SOS 异常检测")
@NameEn("Sos Outlier")
public class SosOutlierBatchOp extends BaseOutlierBatchOp <SosOutlierBatchOp>
	implements SosDetectorParams <SosOutlierBatchOp> {

	public SosOutlierBatchOp() {
		this(null);
	}

	public SosOutlierBatchOp(Params params) {
		super(SosDetector::new, params);
	}

}
