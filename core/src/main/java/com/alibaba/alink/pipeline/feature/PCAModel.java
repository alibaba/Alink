package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.common.feature.pca.PcaModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.feature.PcaPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is pca model.
 */
public class PCAModel extends MapModel<PCAModel>
	implements PcaPredictParams <PCAModel> {

	public PCAModel(Params params) {
		super(PcaModelMapper::new, params);
	}

}
