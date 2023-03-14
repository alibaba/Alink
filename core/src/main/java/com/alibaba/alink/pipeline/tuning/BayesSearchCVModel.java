package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.pipeline.TransformerBase;

@NameCn("Bayes搜索CV模型")
public class BayesSearchCVModel extends BaseTuningModel <BayesSearchCVModel> {

	private static final long serialVersionUID = -3043742268409325004L;

	public BayesSearchCVModel(TransformerBase transformer) {
		super(transformer);
	}

}
