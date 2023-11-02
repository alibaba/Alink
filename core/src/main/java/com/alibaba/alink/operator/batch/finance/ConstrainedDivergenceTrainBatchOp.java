package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.BaseConstrainedLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.finance.ConstrainedLogisticRegressionTrainParams;
import com.alibaba.alink.params.finance.HasConstrainedOptimizationMethod;

@NameCn("带约束 Divergenve 训练")
@NameEn("Constrained Divergence Trainer")
public class ConstrainedDivergenceTrainBatchOp
	extends BaseConstrainedLinearModelTrainBatchOp <ConstrainedDivergenceTrainBatchOp>
	implements ConstrainedLogisticRegressionTrainParams <ConstrainedDivergenceTrainBatchOp> {

	private static final long serialVersionUID = -8070270581529054087L;

	public ConstrainedDivergenceTrainBatchOp(Params params) {
		super(params.clone().set(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD, ConstOptimMethod.ALM),
			LinearModelType.Divergence, "Divergence");
	}

	public ConstrainedDivergenceTrainBatchOp() {
		this(new Params());
	}
}
