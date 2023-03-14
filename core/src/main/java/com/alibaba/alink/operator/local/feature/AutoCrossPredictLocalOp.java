package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.feature.AutoCross.AutoCrossModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.feature.AutoCrossPredictParams;

@NameCn("AutoCross预测")
@NameEn("AutoCross Prediction")
public class AutoCrossPredictLocalOp extends ModelMapLocalOp <AutoCrossPredictLocalOp>
	implements AutoCrossPredictParams <AutoCrossPredictLocalOp> {

	public AutoCrossPredictLocalOp() {
		this(new Params());
	}

	public AutoCrossPredictLocalOp(Params params) {
		super(AutoCrossModelMapper::new, params);
	}
}
