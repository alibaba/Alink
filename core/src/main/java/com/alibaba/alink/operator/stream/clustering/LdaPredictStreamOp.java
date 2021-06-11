package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.LdaModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.LdaPredictParams;

/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 */
public final class LdaPredictStreamOp extends ModelMapStreamOp<LdaPredictStreamOp>
	implements LdaPredictParams <LdaPredictStreamOp> {

	private static final long serialVersionUID = -65065404816427330L;

	public LdaPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public LdaPredictStreamOp(BatchOperator model, Params params) {
		super(model, LdaModelMapper::new, params);
	}

}
