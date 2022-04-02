package com.alibaba.alink.operator.common.fm;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.recommendation.FmPredictParams;

/**
 * fm predict batch operator. this operator predict data's label with fm model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
public final class FmPredictBatchOp extends ModelMapBatchOp <FmPredictBatchOp>
	implements FmPredictParams <FmPredictBatchOp> {

	private static final long serialVersionUID = -2512722656868789290L;

	public FmPredictBatchOp() {
		this(new Params());
	}

	public FmPredictBatchOp(Params params) {
		super(FmModelMapper::new, params);
	}

}
