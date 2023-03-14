package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.SelectorModelMapper;
import com.alibaba.alink.params.finance.SelectorPredictParams;

@ParamSelectColumnSpec(name = "selectedCols", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("Stepwise回归筛选预测")
@NameEn("Regression Selector Predictor")
public class RegressionSelectorPredictBatchOp extends ModelMapBatchOp <RegressionSelectorPredictBatchOp>
	implements SelectorPredictParams <RegressionSelectorPredictBatchOp> {

	private static final long serialVersionUID = 6316651625329228306L;

	public RegressionSelectorPredictBatchOp() {
		this(null);
	}

	public RegressionSelectorPredictBatchOp(Params params) {
		super(SelectorModelMapper::new, params);
	}

}
