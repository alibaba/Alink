package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.classification.ann.MlpcModelMapper;
import com.alibaba.alink.params.classification.MultilayerPerceptronPredictParams;

/**
 * Make prediction based on the multilayer perceptron model fitted by MultilayerPerceptronTrainBatchOp.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("多层感知机分类预测")
public class MultilayerPerceptronPredictBatchOp
	extends ModelMapBatchOp <MultilayerPerceptronPredictBatchOp>
	implements MultilayerPerceptronPredictParams <MultilayerPerceptronPredictBatchOp> {

	private static final long serialVersionUID = 6283020784990848132L;

	public MultilayerPerceptronPredictBatchOp() {
		this(new Params());
	}

	public MultilayerPerceptronPredictBatchOp(Params params) {
		super(MlpcModelMapper::new, params);
	}
}
