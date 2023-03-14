package com.alibaba.alink.pipeline.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierModel;
import com.alibaba.alink.operator.common.outlier.OcsvmModelDetector;

@NameCn("One-Class SVM异常检测模型")
public class OcsvmModelOutlierModel extends BaseModelOutlierModel <OcsvmModelOutlierModel> {

	public OcsvmModelOutlierModel() {
		this(null);
	}

	public OcsvmModelOutlierModel(Params params) {
		super(OcsvmModelDetector::new, params);
	}

}
