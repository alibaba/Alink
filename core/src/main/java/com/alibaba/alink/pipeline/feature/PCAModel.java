package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.pca.PcaModelMapper;
import com.alibaba.alink.params.feature.PcaPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * It is pca model.
 */
@NameCn("主成分分析模型")
public class PCAModel extends MapModel <PCAModel>
	implements PcaPredictParams <PCAModel> {

	private static final long serialVersionUID = 2561211264796620786L;

	public PCAModel() {
		this(null);
	}

	public PCAModel(Params params) {
		super(PcaModelMapper::new, params);
	}

}
