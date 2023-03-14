package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.clustering.LdaModelMapper;
import com.alibaba.alink.params.clustering.LdaPredictParams;

/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("LDA预测")
@NameEn("LDA Prediction")
public final class LdaPredictBatchOp extends ModelMapBatchOp <LdaPredictBatchOp>
	implements LdaPredictParams <LdaPredictBatchOp> {

	private static final long serialVersionUID = -4689226408222779565L;

	/**
	 * Constructor.
	 */
	public LdaPredictBatchOp() {
		this(null);
	}

	/**
	 * Constructor with the algorithm params.
	 */
	public LdaPredictBatchOp(Params params) {
		super(LdaModelMapper::new, params);
	}

}
