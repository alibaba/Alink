package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.Id3PredictParams;

/**
 * The batch operator that predict the data using the id3 model.
 */
@NameCn("ID3决策树分类预测")
public final class Id3PredictBatchOp extends ModelMapBatchOp <Id3PredictBatchOp> implements
	Id3PredictParams <Id3PredictBatchOp> {
	private static final long serialVersionUID = 7494960454797499134L;

	public Id3PredictBatchOp() {
		this(null);
	}

	public Id3PredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}
}
