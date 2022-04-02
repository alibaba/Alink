package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.params.classification.GbdtPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * The model of gbdt classification.
 */
@NameCn("GBDT分类器模型")
public class GbdtClassificationModel extends MapModel <GbdtClassificationModel>
	implements GbdtPredictParams <GbdtClassificationModel> {

	private static final long serialVersionUID = -4935113216223290008L;

	public GbdtClassificationModel() {this(null);}

	public GbdtClassificationModel(Params params) {
		super(GbdtModelMapper::new, params);
	}

}
