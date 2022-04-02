package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.params.recommendation.FmPredictParams;

/**
 * Fm predict batch operator. this operator predict data's label with fm model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("FM回归预测")
public final class FmRegressorPredictBatchOp extends ModelMapBatchOp <FmRegressorPredictBatchOp>
	implements FmPredictParams <FmRegressorPredictBatchOp> {

	private static final long serialVersionUID = -1174383656892662407L;

	public FmRegressorPredictBatchOp() {
		this(new Params());
	}

	public FmRegressorPredictBatchOp(Params params) {
		super(FmModelMapper::new, params);
	}

}
