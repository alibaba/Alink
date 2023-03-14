package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.params.feature.TreeModelEncoderParams;

/**
 * TreeModel encoder to encode tree model to sparse matrix which is used
 * as feature for classifier or regressor.
 */
@NameCn("决策树模型编码")
@NameEn("Tree Model Encoder")
public class TreeModelEncoderBatchOp extends ModelMapBatchOp <TreeModelEncoderBatchOp>
	implements TreeModelEncoderParams <TreeModelEncoderBatchOp> {
	private static final long serialVersionUID = -7596799114572234171L;

	public TreeModelEncoderBatchOp() {
		this(null);
	}

	public TreeModelEncoderBatchOp(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}
}
