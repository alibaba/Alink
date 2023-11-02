package com.alibaba.alink.pipeline.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.finance.group.SimpleGroupScorecardModelMapper;
import com.alibaba.alink.params.finance.GroupScorecardPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("")
@Internal
public class SimpleGroupScoreModel extends MapModel <SimpleGroupScoreModel>
	implements GroupScorecardPredictParams <SimpleGroupScoreModel> {

	public SimpleGroupScoreModel() {
		this(new Params());
	}

	public SimpleGroupScoreModel(Params params) {
		super(SimpleGroupScorecardModelMapper::new, params);
	}
}
