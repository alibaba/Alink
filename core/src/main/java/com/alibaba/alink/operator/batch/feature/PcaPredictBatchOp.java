package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.feature.pca.PcaModelMapper;
import com.alibaba.alink.params.feature.PcaPredictParams;

/**
 * pca predict for batch data, it need a pca model which is train from PcaTrainBatchOp
 */
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("主成分分析预测")
@NameEn("Pca Prediction")
public class PcaPredictBatchOp extends ModelMapBatchOp <PcaPredictBatchOp>
	implements PcaPredictParams <PcaPredictBatchOp> {

	private static final long serialVersionUID = -3828246915786937109L;

	/**
	 * default constructor
	 */
	public PcaPredictBatchOp() {
		this(null);
	}

	/**
	 * constructor.
	 *
	 * @param params parameter set.
	 */
	public PcaPredictBatchOp(Params params) {
		super(PcaModelMapper::new, params);
	}
}
