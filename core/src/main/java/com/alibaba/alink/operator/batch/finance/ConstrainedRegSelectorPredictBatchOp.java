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
@NameCn("带约束的Stepwise回归筛选预测")
@NameEn("Constrained Linear Selector Predictor")
public class ConstrainedRegSelectorPredictBatchOp extends ModelMapBatchOp <ConstrainedRegSelectorPredictBatchOp>
	implements SelectorPredictParams <ConstrainedRegSelectorPredictBatchOp> {

	private static final long serialVersionUID = 152592469378068233L;

	public ConstrainedRegSelectorPredictBatchOp() {
		this(null);
	}

	public ConstrainedRegSelectorPredictBatchOp(Params params) {
		super(SelectorModelMapper::new, params);
	}

}
