package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.OneHotModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.feature.OneHotPredictParams;

/**
 * One-hot batch operator maps a serial of columns of category indices to a column of
 * sparse binary vectors.
 */
public final class OneHotPredictBatchOp extends ModelMapBatchOp <OneHotPredictBatchOp>
	implements OneHotPredictParams <OneHotPredictBatchOp> {

	/**
	 * constructor.
	 */
	public OneHotPredictBatchOp() {
		this(null);
	}

	/**
	 * constructor.
	 *
	 * @param params parameter set.
	 */
	public OneHotPredictBatchOp(Params params) {
		super(OneHotModelMapper::new, params);
	}
}
