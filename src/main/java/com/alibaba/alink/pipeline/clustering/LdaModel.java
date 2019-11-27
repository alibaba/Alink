package com.alibaba.alink.pipeline.clustering;

import com.alibaba.alink.operator.common.clustering.LdaModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.clustering.LdaPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Lda model.
 */
public class LdaModel extends MapModel<LdaModel>
	implements LdaPredictParams <LdaModel> {

	public LdaModel(Params params) {
		super(LdaModelMapper::new, params);
	}

}