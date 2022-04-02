package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.recommendation.FmPredictParams;

/**
 * Fm pipeline model.
 */
@NameCn("FM分类模型")
public class FmClassificationModel extends FmModel <FmClassificationModel>
	implements FmPredictParams <FmClassificationModel> {
	private static final long serialVersionUID = 8702278778833625190L;

	public FmClassificationModel() {this(null);}

	public FmClassificationModel(Params params) {
		super(params);
	}
}
