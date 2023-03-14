package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.LdaModelMapper;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.LdaPredictParams;

/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("LDA预测")
@NameEn("LDA Prediction")
public final class LdaPredictStreamOp extends ModelMapStreamOp<LdaPredictStreamOp>
	implements LdaPredictParams <LdaPredictStreamOp> {

	private static final long serialVersionUID = -65065404816427330L;

	public LdaPredictStreamOp() {
		super(LdaModelMapper::new, new Params());
	}

	public LdaPredictStreamOp(Params params) {
		super(LdaModelMapper::new, params);
	}

	public LdaPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public LdaPredictStreamOp(BatchOperator model, Params params) {
		super(model, LdaModelMapper::new, params);
	}

}
