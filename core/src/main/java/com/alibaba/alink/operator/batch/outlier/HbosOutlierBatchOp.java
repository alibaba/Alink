package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierBatchOp;
import com.alibaba.alink.operator.common.outlier.HbosDetector;
import com.alibaba.alink.params.outlier.HbosDetectorParams;

@NameCn("HBOS异常检测")
@NameEn("HBOS Outlier")
public class HbosOutlierBatchOp extends BaseOutlierBatchOp <HbosOutlierBatchOp>
	implements HbosDetectorParams <HbosOutlierBatchOp> {

	public HbosOutlierBatchOp() {
		this(null);
	}

	public HbosOutlierBatchOp(Params params) {
		super(HbosDetector::new, params);
	}

}
