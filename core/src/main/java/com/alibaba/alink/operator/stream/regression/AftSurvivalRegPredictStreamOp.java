package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.regression.AFTModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.AftRegPredictParams;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("生存回归预测")
public class AftSurvivalRegPredictStreamOp extends ModelMapStreamOp <AftSurvivalRegPredictStreamOp>
	implements AftRegPredictParams <AftSurvivalRegPredictStreamOp> {

	private static final long serialVersionUID = -4560738919934088694L;

	/**
	 * Constructor.
	 */
	public AftSurvivalRegPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	/**
	 * Constructor with the algorithm params.
	 */
	public AftSurvivalRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, AFTModelMapper::new, params);
	}

}
