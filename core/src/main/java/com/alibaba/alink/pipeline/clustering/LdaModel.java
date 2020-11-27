package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.clustering.LdaModelMapper;
import com.alibaba.alink.params.clustering.LdaPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Lda pipeline model.
 */
public class LdaModel extends MapModel <LdaModel>
	implements LdaPredictParams <LdaModel> {

	private static final long serialVersionUID = 2842503322990552690L;

	public LdaModel(Params params) {
		super(LdaModelMapper::new, params);
	}

}