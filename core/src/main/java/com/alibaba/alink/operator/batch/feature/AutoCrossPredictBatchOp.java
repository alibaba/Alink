package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.AutoCross.AutoCrossModelMapper;
import com.alibaba.alink.params.feature.AutoCrossPredictParams;

@NameCn("AutoCross预测")
@NameEn("AutoCross Prediction")
public class AutoCrossPredictBatchOp extends ModelMapBatchOp <AutoCrossPredictBatchOp>
	implements AutoCrossPredictParams <AutoCrossPredictBatchOp> {

	private static final long serialVersionUID = 3987270029076248190L;

	public AutoCrossPredictBatchOp() {
		this(new Params());
	}

	public AutoCrossPredictBatchOp(Params params) {
		super(AutoCrossModelMapper::new, params);
	}
}
