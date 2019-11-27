package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.classification.OneVsRestModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.OneVsRestPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by OneVsRest.
 */
public class OneVsRestModel extends MapModel<OneVsRestModel>
	implements OneVsRestPredictParams <OneVsRestModel> {

	public OneVsRestModel() {
		this(null);
	}

	public OneVsRestModel(Params params) {
		super(OneVsRestModelMapper::new, params);
	}
}