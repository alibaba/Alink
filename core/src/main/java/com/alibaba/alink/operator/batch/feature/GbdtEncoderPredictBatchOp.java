package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.TreeModelEncoderParams;

/**
 * Gbdt encoder to encode gbdt model to sparse matrix which is used
 * as feature for classifier.
 */
@NameCn("GBDT分类编码预测")
@NameEn("Gbdt Encoder Prediction")
public class GbdtEncoderPredictBatchOp extends ModelMapBatchOp <GbdtEncoderPredictBatchOp>
	implements TreeModelEncoderParams <GbdtEncoderPredictBatchOp> {
	private static final long serialVersionUID = -7596799178072234171L;

	public GbdtEncoderPredictBatchOp() {
		this(new Params());
	}

	public GbdtEncoderPredictBatchOp(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}
}
