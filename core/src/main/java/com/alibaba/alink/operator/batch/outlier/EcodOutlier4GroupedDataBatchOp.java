package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.params.outlier.CopodDetectorParams;
import com.alibaba.alink.operator.common.outlier.EcodDetector;

@NameCn("ECOD序列异常检测")
@NameEn("ECOD Series Outlier")
public class EcodOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <EcodOutlier4GroupedDataBatchOp>
	implements CopodDetectorParams <EcodOutlier4GroupedDataBatchOp> {

	public EcodOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public EcodOutlier4GroupedDataBatchOp(Params params) {
		super(EcodDetector::new, params);
	}
}