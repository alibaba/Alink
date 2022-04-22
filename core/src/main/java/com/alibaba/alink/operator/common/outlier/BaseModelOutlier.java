package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.outlier.ModelOutlierParams;
import com.alibaba.alink.pipeline.Trainer;

public abstract class BaseModelOutlier<T extends BaseModelOutlier <T, M>, M extends BaseModelOutlierModel <M>>
	extends Trainer <T, M>
	implements ModelOutlierParams <T> {

	public BaseModelOutlier(Params params) {
		super(params);
	}

}
