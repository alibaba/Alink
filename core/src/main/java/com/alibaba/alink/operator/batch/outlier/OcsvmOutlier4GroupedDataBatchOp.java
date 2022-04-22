package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.common.outlier.OcsvmDetector;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

@NameCn("One-Class SVM分组异常检测")
public class OcsvmOutlier4GroupedDataBatchOp extends BaseOutlier4GroupedDataBatchOp <OcsvmOutlier4GroupedDataBatchOp>
	implements OcsvmDetectorParams <OcsvmOutlier4GroupedDataBatchOp> {

	public OcsvmOutlier4GroupedDataBatchOp() {
		this(null);
	}

	public OcsvmOutlier4GroupedDataBatchOp(Params params) {
		super(OcsvmDetector::new, params);
	}

}
