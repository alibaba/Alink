package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.regression.AFTModelMapper;
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
public class AftSurvivalRegPredictBatchOp extends ModelMapBatchOp <AftSurvivalRegPredictBatchOp>
	implements AftRegPredictParams <AftSurvivalRegPredictBatchOp> {

	private static final long serialVersionUID = 8298656899275333697L;

	/**
	 * Constructor.
	 */
	public AftSurvivalRegPredictBatchOp() {
		this(new Params());
	}

	/**
	 * Constructor with the algorithm params.
	 */
	public AftSurvivalRegPredictBatchOp(Params params) {
		super(AFTModelMapper::new, params);
	}
}
