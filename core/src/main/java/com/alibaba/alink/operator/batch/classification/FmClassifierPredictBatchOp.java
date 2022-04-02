package com.alibaba.alink.operator.batch.classification;

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
@NameCn("FM分类预测")
public final class FmClassifierPredictBatchOp extends ModelMapBatchOp <FmClassifierPredictBatchOp>
	implements FmPredictParams <FmClassifierPredictBatchOp> {

	private static final long serialVersionUID = 1792006486519777948L;

	public FmClassifierPredictBatchOp() {
		this(new Params());
	}

	public FmClassifierPredictBatchOp(Params params) {
		super(FmModelMapper::new, params);
	}

}
