package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.recommendation.FmRecommBinaryImplicitTrainParams;

/**
 * Fm train batch op for implicit rating condition.
 */
public final class FmRecommBinaryImplicitTrainBatchOp
	extends BatchOperator <FmRecommBinaryImplicitTrainBatchOp>
	implements FmRecommBinaryImplicitTrainParams <FmRecommBinaryImplicitTrainBatchOp> {

	private static final long serialVersionUID = -1520956117231532530L;

	public FmRecommBinaryImplicitTrainBatchOp() {
		this(new Params());
	}

	public FmRecommBinaryImplicitTrainBatchOp(Params params) {
		super(params);
	}

	/**
	 * There are 3 input tables: 1) user-item-label table, 2) user features table, 3) item features table.
	 * If user or item features table is missing, then use their IDs as features.
	 */
	@Override
	public FmRecommBinaryImplicitTrainBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> samplesOp = inputs[0];
		final Long envId = samplesOp.getMLEnvironmentId();
		BatchOperator <?> userFeaturesOp = inputs.length >= 2 ? inputs[1] : null;
		BatchOperator <?> itemFeaturesOp = inputs.length >= 3 ? inputs[2] : null;
		Params params = getParams().clone();

		String userCol = params.get(USER_COL);
		String itemCol = params.get(ITEM_COL);
		String rateCol = params.get(RATE_COL);

		if (rateCol == null) {
			samplesOp = new NegativeItemSamplingBatchOp().linkFrom(samplesOp.select(new String[] {userCol, itemCol}));
			String labelCol = samplesOp.getColNames()[2];
			params.set(RATE_COL, labelCol);
		}

		FmRecommTrainBatchOp fmRecommTrainBatchOp = new FmRecommTrainBatchOp(params)
			.setMLEnvironmentId(envId);
		fmRecommTrainBatchOp.implicitFeedback = true;

		fmRecommTrainBatchOp.linkFrom(samplesOp, userFeaturesOp, itemFeaturesOp);
		setOutputTable(fmRecommTrainBatchOp.getOutputTable());
		return this;
	}
}
