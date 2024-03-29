package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.OcsvmDetector;
import com.alibaba.alink.params.outlier.OcsvmDetectorParams;

@NameCn("One-Class SVM异常检测")
@NameEn("One-Class SVM Outlier Detection")
public class OcsvmOutlierLocalOp extends BaseOutlierLocalOp <OcsvmOutlierLocalOp>
	implements OcsvmDetectorParams <OcsvmOutlierLocalOp> {

	public OcsvmOutlierLocalOp() {
		this(null);
	}

	public OcsvmOutlierLocalOp(Params params) {
		super(OcsvmDetector::new, params);
	}

}
