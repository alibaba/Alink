package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.SelectedColsWithSecondInputSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.params.feature.QuantileDiscretizerPredictParams;

/**
 * EqualWidth discretizer keeps every interval the same width, output the interval
 * as model, and can transform a new data using the model.
 * <p>The output is the index of the interval.
 */
@SelectedColsWithSecondInputSpec
@NameCn("等宽离散化预测")
public final class EqualWidthDiscretizerPredictBatchOp extends ModelMapBatchOp <EqualWidthDiscretizerPredictBatchOp>
	implements QuantileDiscretizerPredictParams <EqualWidthDiscretizerPredictBatchOp> {

	private static final long serialVersionUID = -3438278525598601460L;

	public EqualWidthDiscretizerPredictBatchOp() {
		this(null);
	}

	public EqualWidthDiscretizerPredictBatchOp(Params params) {
		super(QuantileDiscretizerModelMapper::new, params);
	}
}
