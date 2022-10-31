package com.alibaba.alink.pipeline.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlierModel;
import com.alibaba.alink.operator.common.outlier.IForestModelDetector;

@NameCn("IForest异常检测模型")
@NameEn("IForest outlier model")
public class IForestModelOutlierModel extends BaseModelOutlierModel <IForestModelOutlierModel> {

	public IForestModelOutlierModel() {
		this(null);
	}

	public IForestModelOutlierModel(Params params) {
		super(IForestModelDetector::new, params);
	}

}
