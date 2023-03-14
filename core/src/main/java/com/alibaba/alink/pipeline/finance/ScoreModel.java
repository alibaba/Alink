package com.alibaba.alink.pipeline.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.finance.ScorePredictMapper;
import com.alibaba.alink.params.finance.ScorePredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("")
public class ScoreModel extends MapModel <ScoreModel>
	implements ScorePredictParams <ScoreModel> {
	private static final long serialVersionUID = 713206402279384736L;

	public ScoreModel() {
		this(new Params());
	}

	public ScoreModel(Params params) {
		super(ScorePredictMapper::new, params);
	}
}
