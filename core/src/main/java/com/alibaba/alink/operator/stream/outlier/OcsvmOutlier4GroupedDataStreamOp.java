package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlier4GroupedDataStreamOp;
import com.alibaba.alink.operator.common.outlier.OcsvmDetector;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

@NameCn("One-Class SVM流式分组异常检测")
@NameEn("One-class svm outlier for grouped data prediction")
public class OcsvmOutlier4GroupedDataStreamOp extends BaseOutlier4GroupedDataStreamOp <OcsvmOutlier4GroupedDataStreamOp>
	implements OcsvmDetectorParams <OcsvmOutlier4GroupedDataStreamOp> {

	public OcsvmOutlier4GroupedDataStreamOp() {
		this(null);
	}

	public OcsvmOutlier4GroupedDataStreamOp(Params params) {
		super(OcsvmDetector::new, params);
	}

}
