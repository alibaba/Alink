package com.alibaba.alink.pipeline.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.finance.group.GroupScorecardModelMapper;
import com.alibaba.alink.params.finance.GroupScorecardPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("")
public class GroupScoreModel extends MapModel <GroupScoreModel>
	implements GroupScorecardPredictParams <GroupScoreModel> {

	public GroupScoreModel() {
		this(new Params());
	}

	public GroupScoreModel(Params params) {
		super(GroupScorecardModelMapper::new, params);
	}
}
