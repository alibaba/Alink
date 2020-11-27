package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.C45PredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * the c45 model for pipeline.
 */
public class C45Model extends MapModel <C45Model>
	implements C45PredictParams <C45Model> {

	private static final long serialVersionUID = 3446051414242288960L;

	public C45Model() {this(null);}

	public C45Model(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}