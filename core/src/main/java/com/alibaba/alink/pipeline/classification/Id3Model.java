package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.Id3PredictParams;
import com.alibaba.alink.pipeline.MapModel;


/**
 * the id3 model for pipeline.
 */
@NameCn("ID3决策树分类模型")
public class Id3Model extends MapModel <Id3Model>
	implements Id3PredictParams <Id3Model> {

	private static final long serialVersionUID = 1303892137143865652L;

	public Id3Model() {this(null);}

	public Id3Model(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
