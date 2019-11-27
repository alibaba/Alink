package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.pca.PcaModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.PcaPredictParams;

/**
 * pca predict for stream data, it need a pca model which is train from PcaTrainBatchOp
 */
public final class PcaPredictStreamOp extends ModelMapStreamOp <PcaPredictStreamOp>
	implements PcaPredictParams <PcaPredictStreamOp> {

	/**
	 * default constructor
	 *
	 * @param model train from PcaTrainBatchOp
	 */
	public PcaPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public PcaPredictStreamOp(BatchOperator model, Params params) {
		super(model, PcaModelMapper::new, params);
	}

}