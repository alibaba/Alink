package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.params.recommendation.FmPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Fm pipeline model.
 */
public class FmModel extends MapModel <FmModel>
	implements FmPredictParams <FmModel> {

	private static final long serialVersionUID = 8702278778833625190L;

	public FmModel() {this(null);}

	public FmModel(Params params) {
		super(FmModelMapper::new, params);
	}

}
