package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;

/**
 * The batch operator that predict the data using the quantile discretizer model.
 */
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("分位数离散化预测")
@NameEn("Quantile Discretizer Prediction")
public final class QuantileDiscretizerPredictBatchOp extends ModelMapBatchOp <QuantileDiscretizerPredictBatchOp>
	implements QuantileDiscretizerPredictParams <QuantileDiscretizerPredictBatchOp> {

	private static final long serialVersionUID = -8459276043199364983L;

	public QuantileDiscretizerPredictBatchOp() {
		this(null);
	}

	public QuantileDiscretizerPredictBatchOp(Params params) {
		super(QuantileDiscretizerModelMapper::new, params);
	}
}
