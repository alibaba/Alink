package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.pipeline.TransformerBase;

/**
 * model of random search cv.
 */
@NameCn("随机搜索CV模型")
public class RandomSearchCVModel extends BaseTuningModel <RandomSearchCVModel> {

	private static final long serialVersionUID = -6349375937046890931L;

	public RandomSearchCVModel(TransformerBase transformer) {
		super(transformer);
	}

}
