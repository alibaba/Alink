package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.tree.BaseXGBoostTrainBatchOp;
import com.alibaba.alink.params.xgboost.XGBoostTrainParams;

@NameCn("XGBoost二分类训练")
@NameEn("XGBoost Binary Classification Training")
public final class XGBoostTrainBatchOp extends BaseXGBoostTrainBatchOp <XGBoostTrainBatchOp>
	implements XGBoostTrainParams <XGBoostTrainBatchOp> {

	public XGBoostTrainBatchOp() {
	}

	public XGBoostTrainBatchOp(Params params) {
		super(params);
	}
}
