package com.alibaba.alink.pipeline.clustering;

import com.alibaba.alink.operator.common.clustering.GmmModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.clustering.GmmPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by GaussianMixture.
 */
public class GaussianMixtureModel extends MapModel<GaussianMixtureModel>
	implements GmmPredictParams <GaussianMixtureModel> {

	public GaussianMixtureModel() {this(null);}

	public GaussianMixtureModel(Params params) {
		super(GmmModelMapper::new, params);
	}

}
