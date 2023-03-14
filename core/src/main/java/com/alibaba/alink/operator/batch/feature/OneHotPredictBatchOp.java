package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.OneHotModelMapper;
import com.alibaba.alink.params.feature.OneHotPredictParams;

/**
 * One-hot batch operator maps a serial of columns of category indices to a column of
 * sparse binary vectors.
 */
@NameCn("独热编码预测")
@NameEn("OneHot Encoder Predict")
@ParamSelectColumnSpec(name = "selectedCols", portIndices = {1})
public final class OneHotPredictBatchOp extends ModelMapBatchOp <OneHotPredictBatchOp>
	implements OneHotPredictParams <OneHotPredictBatchOp> {

	private static final long serialVersionUID = -6881814793972704312L;

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
