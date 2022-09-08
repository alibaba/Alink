package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;

/**
 * Softmax predict batch operator.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("Softmax预测")
@NameEn("Softmax Prediction")
public final class SoftmaxPredictBatchOp extends ModelMapBatchOp <SoftmaxPredictBatchOp>
	implements SoftmaxPredictParams <SoftmaxPredictBatchOp> {

	private static final long serialVersionUID = -211305912539188917L;

	public SoftmaxPredictBatchOp() {
		this(new Params());
	}

	public SoftmaxPredictBatchOp(Params params) {
		super(SoftmaxModelMapper::new, params);
	}

}
