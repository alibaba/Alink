package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.clustering.GmmModelMapper;
import com.alibaba.alink.params.clustering.GmmPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Model fitted by GaussianMixture.
 */
@NameCn("高斯混合模型")
public class GaussianMixtureModel extends MapModel <GaussianMixtureModel>
	implements GmmPredictParams <GaussianMixtureModel> {

	private static final long serialVersionUID = -5876777057343526932L;

	public GaussianMixtureModel() {this(null);}

	public GaussianMixtureModel(Params params) {
		super(GmmModelMapper::new, params);
	}

}
