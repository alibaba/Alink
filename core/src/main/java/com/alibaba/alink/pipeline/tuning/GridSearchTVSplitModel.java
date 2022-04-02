package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.pipeline.TransformerBase;

/**
 * model of grid search tv split.
 */
@NameCn("网格搜索TV模型")
public class GridSearchTVSplitModel extends BaseTuningModel <GridSearchTVSplitModel> {

	private static final long serialVersionUID = 7538357490813799093L;

	public GridSearchTVSplitModel(TransformerBase transformer) {
		super(transformer);
	}

}
