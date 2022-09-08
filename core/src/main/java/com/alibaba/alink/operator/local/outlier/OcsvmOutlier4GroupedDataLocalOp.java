package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.outlier.OcsvmDetector;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

@NameCn("One-Class SVM分组异常检测")
public class OcsvmOutlier4GroupedDataLocalOp extends BaseOutlier4GroupedDataLocalOp <OcsvmOutlier4GroupedDataLocalOp>
	implements OcsvmDetectorParams <OcsvmOutlier4GroupedDataLocalOp> {

	public OcsvmOutlier4GroupedDataLocalOp() {
		this(null);
	}

	public OcsvmOutlier4GroupedDataLocalOp(Params params) {
		super(OcsvmDetector::new, params);
	}

}
