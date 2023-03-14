package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.pipeline.TransformerBase;

@NameCn("Bayes搜索TV模型")
public class BayesSearchTVSplitModel extends BaseTuningModel <BayesSearchTVSplitModel> {

	private static final long serialVersionUID = -3886475078182230126L;

	public BayesSearchTVSplitModel(TransformerBase transformer) {
		super(transformer);
	}

}
