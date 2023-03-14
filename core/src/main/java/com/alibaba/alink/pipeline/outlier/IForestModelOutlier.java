package com.alibaba.alink.pipeline.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.outlier.BaseModelOutlier;
import com.alibaba.alink.params.outlier.IForestModelTrainParams;

@NameCn("IForest异常检测")
@NameEn("IForest outlier")
public class IForestModelOutlier extends BaseModelOutlier <IForestModelOutlier, IForestModelOutlierModel>
	implements IForestModelTrainParams <IForestModelOutlier> {

	public IForestModelOutlier() {
		this(null);
	}

	public IForestModelOutlier(Params params) {
		super(params);
	}

}
