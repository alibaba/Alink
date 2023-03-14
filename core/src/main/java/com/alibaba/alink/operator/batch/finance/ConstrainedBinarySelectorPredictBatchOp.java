package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.SelectorModelMapper;
import com.alibaba.alink.params.finance.SelectorPredictParams;

@ParamSelectColumnSpec(name = "selectedCol")
@NameCn("带约束的Stepwise二分类筛选预测")
@NameEn("Constrained Binary Selector Predictor")
public class ConstrainedBinarySelectorPredictBatchOp extends ModelMapBatchOp <ConstrainedBinarySelectorPredictBatchOp>
	implements SelectorPredictParams <ConstrainedBinarySelectorPredictBatchOp> {

	private static final long serialVersionUID = -6112139129156096897L;

	public ConstrainedBinarySelectorPredictBatchOp() {
		this(null);
	}

	public ConstrainedBinarySelectorPredictBatchOp(Params params) {
		super(SelectorModelMapper::new, params);
	}

}
