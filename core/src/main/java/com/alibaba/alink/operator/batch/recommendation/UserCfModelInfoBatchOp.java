package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.common.recommendation.UserCfModelInfo;

import java.util.List;

public class UserCfModelInfoBatchOp extends ExtractModelInfoBatchOp <UserCfModelInfo, UserCfModelInfoBatchOp> {
	private static final long serialVersionUID = 1735133462550836751L;

	public UserCfModelInfoBatchOp() {
		this(null);
	}

	public UserCfModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	public UserCfModelInfo createModelInfo(List <Row> rows) {
		return new UserCfModelInfo(rows);
	}
}
