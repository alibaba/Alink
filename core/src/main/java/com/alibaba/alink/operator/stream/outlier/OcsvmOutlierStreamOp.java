package com.alibaba.alink.operator.stream.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseOutlierStreamOp;
import com.alibaba.alink.operator.common.outlier.OcsvmDetector;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

@NameCn("One-Class SVM流式异常检测")
@NameEn("One-class svm outlier")
public class OcsvmOutlierStreamOp extends BaseOutlierStreamOp <OcsvmOutlierStreamOp>
	implements OcsvmDetectorParams <OcsvmOutlierStreamOp> {

	public OcsvmOutlierStreamOp() {
		this(null);
	}

	public OcsvmOutlierStreamOp(Params params) {
		super(OcsvmDetector::new, params);
	}

}
