package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.OneHotModelMapper;
import com.alibaba.alink.params.feature.OneHotPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * One hot pipeline model.
 */
public class OneHotEncoderModel extends MapModel <OneHotEncoderModel>
	implements OneHotPredictParams <OneHotEncoderModel> {

	private static final long serialVersionUID = 650547453981086250L;

	public OneHotEncoderModel() {
		this(null);
	}

	public OneHotEncoderModel(Params params) {
		super(OneHotModelMapper::new, params);
	}

}
