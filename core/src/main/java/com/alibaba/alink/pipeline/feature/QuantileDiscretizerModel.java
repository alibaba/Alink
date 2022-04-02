package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is quantile discretizer model.
 */
@NameCn("分位数离散化模型")
public class QuantileDiscretizerModel extends MapModel <QuantileDiscretizerModel>
	implements QuantileDiscretizerPredictParams <QuantileDiscretizerModel> {

	private static final long serialVersionUID = -7668583614855516255L;
	
	public QuantileDiscretizerModel() {
		this(null);
	}

	public QuantileDiscretizerModel(Params params) {
		super(QuantileDiscretizerModelMapper::new, params);
	}
}
