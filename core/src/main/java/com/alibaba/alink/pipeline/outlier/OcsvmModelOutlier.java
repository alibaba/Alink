package com.alibaba.alink.pipeline.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlier;
import com.alibaba.alink.params.outlier.OcsvmModelTrainParams;

@NameCn("One-Class SVM异常检测")
public class OcsvmModelOutlier extends BaseModelOutlier <OcsvmModelOutlier, OcsvmModelOutlierModel>
	implements OcsvmModelTrainParams <OcsvmModelOutlier> {

	public OcsvmModelOutlier() {
		this(null);
	}

	public OcsvmModelOutlier(Params params) {
		super(params);
	}

}
