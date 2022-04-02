package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.params.classification.GbdtPredictParams;

/**
 * The batch operator that predict the data using the binary gbdt model.
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("GBDT分类器预测")
public final class GbdtPredictBatchOp extends ModelMapBatchOp <GbdtPredictBatchOp>
	implements GbdtPredictParams <GbdtPredictBatchOp> {
	private static final long serialVersionUID = 2801048935862838531L;

	public GbdtPredictBatchOp() {
		this(null);
	}

	public GbdtPredictBatchOp(Params params) {
		super(GbdtModelMapper::new, params);
	}
}
