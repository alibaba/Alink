package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.pipeline.TransformerBase;

/**
 * model of grid search cv.
 */
@NameCn("网格搜索CV模型")
public class GridSearchCVModel extends BaseTuningModel <GridSearchCVModel> {

	private static final long serialVersionUID = -1418839349445748450L;

	public GridSearchCVModel(TransformerBase transformer) {
		super(transformer);
	}

}
