package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.classification.OneVsRestModelMapper;
import com.alibaba.alink.params.classification.OneVsRestPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by OneVsRest.
 */
public class OneVsRestModel extends MapModel <OneVsRestModel>
	implements OneVsRestPredictParams <OneVsRestModel> {

	private static final long serialVersionUID = 2876335899659612299L;

	public OneVsRestModel() {
		this(null);
	}

	public OneVsRestModel(Params params) {
		super(OneVsRestModelMapper::new, params);
	}
}