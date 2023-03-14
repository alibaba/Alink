package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.operator.common.timeseries.ProphetModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;

/**
 * The stream operator that predict the data using the quantile discretizer model.
 */
@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("分位数离散化预测")
@NameEn("Quantile discretizer prediction")
public class QuantileDiscretizerPredictStreamOp extends ModelMapStreamOp <QuantileDiscretizerPredictStreamOp>
	implements QuantileDiscretizerPredictParams <QuantileDiscretizerPredictStreamOp> {

	private static final long serialVersionUID = 6454721782371885502L;

	public QuantileDiscretizerPredictStreamOp() {
		super(QuantileDiscretizerModelMapper::new, new Params());
	}

	public QuantileDiscretizerPredictStreamOp(Params params) {
		super(QuantileDiscretizerModelMapper::new, params);
	}

	public QuantileDiscretizerPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public QuantileDiscretizerPredictStreamOp(BatchOperator model, Params params) {
		super(model, QuantileDiscretizerModelMapper::new, params);
	}
}
