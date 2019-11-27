package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class QuantileDiscretizerModel extends MapModel<QuantileDiscretizerModel>
	implements QuantileDiscretizerPredictParams <QuantileDiscretizerModel> {

	public QuantileDiscretizerModel(Params params) {
		super(QuantileDiscretizerModelMapper::new, params);
	}
}
