package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.finance.ScorecardModelMapper;
import com.alibaba.alink.params.finance.ScorecardPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("评分卡模型")
public class ScorecardModel extends MapModel <ScorecardModel>
	implements ScorecardPredictParams <ScorecardModel> {

	private static final long serialVersionUID = -6327950527201298541L;

	public ScorecardModel() {
		this(null);
	}

	public ScorecardModel(Params params) {
		super(ScorecardModelMapper::new, params);
	}
}
