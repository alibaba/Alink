package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.finance.stepwiseSelector.SelectorModelMapper;
import com.alibaba.alink.params.finance.SelectorPredictParams;

@ParamSelectColumnSpec(name = "selectedCol")
@NameCn("Stepwise二分类筛选预测")
@NameEn("Stepwise Binary Selector Predictor")
public class BinarySelectorPredictBatchOp extends ModelMapBatchOp <BinarySelectorPredictBatchOp>
	implements SelectorPredictParams <BinarySelectorPredictBatchOp> {

	private static final long serialVersionUID = -801930887556428652L;

	public BinarySelectorPredictBatchOp() {
		this(null);
	}

	public BinarySelectorPredictBatchOp(Params params) {
		super(SelectorModelMapper::new, params);
	}

}
