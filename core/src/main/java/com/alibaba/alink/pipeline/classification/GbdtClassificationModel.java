package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.GbdtPredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class GbdtClassificationModel extends MapModel<GbdtClassificationModel>
	implements GbdtPredictParams <GbdtClassificationModel> {

	public GbdtClassificationModel() {this(null);}

	public GbdtClassificationModel(Params params) {
		super(GbdtModelMapper::new, params);
	}

}