package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.OneHotModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.feature.OneHotPredictParams;

/**
 * One-hot batch operator maps a serial of columns of category indices to a column of
 * sparse binary vectors.
 */
@NameCn("独热编码预测")
public final class OneHotPredictLocalOp extends ModelMapLocalOp <OneHotPredictLocalOp>
	implements OneHotPredictParams <OneHotPredictLocalOp> {

	/**
	 * constructor.
	 */
	public OneHotPredictLocalOp() {
		this(null);
	}

	/**
	 * constructor.
	 *
	 * @param params parameter set.
	 */
	public OneHotPredictLocalOp(Params params) {
		super(OneHotModelMapper::new, params);
	}
}
