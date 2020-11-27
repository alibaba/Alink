package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.clustering.LdaModelMapper;
import com.alibaba.alink.params.clustering.LdaPredictParams;

/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 */
public final class LdaPredictStreamOp extends ModelMapBatchOp <LdaPredictStreamOp>
	implements LdaPredictParams <LdaPredictStreamOp> {

	private static final long serialVersionUID = -65065404816427330L;

	/**
	 * Constructor.
	 */
	public LdaPredictStreamOp() {
		this(null);
	}

	/**
	 * Constructor with the algorithm params.
	 */
	public LdaPredictStreamOp(Params params) {
		super(LdaModelMapper::new, params);
	}

}
