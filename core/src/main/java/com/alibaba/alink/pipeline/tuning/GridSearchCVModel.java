package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.pipeline.TransformerBase;

public class GridSearchCVModel extends BaseTuningModel <GridSearchCVModel> {

	public GridSearchCVModel(TransformerBase transformer, Report report) {
		super(transformer, report);
	}

}
