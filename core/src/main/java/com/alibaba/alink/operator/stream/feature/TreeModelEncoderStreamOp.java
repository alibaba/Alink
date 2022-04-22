package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.TreeModelEncoderModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.feature.TreeModelEncoderParams;

/**
 * TreeModel encoder to encode tree model to sparse matrix which is used
 * as feature for classifier or regressor.
 */
@NameCn("流式树模型编码")
public class TreeModelEncoderStreamOp extends ModelMapStreamOp <TreeModelEncoderStreamOp>
	implements TreeModelEncoderParams <TreeModelEncoderStreamOp> {
	private static final long serialVersionUID = -6491544498862384079L;

	public TreeModelEncoderStreamOp() {
		super(TreeModelEncoderModelMapper::new, new Params());
	}

	public TreeModelEncoderStreamOp(Params params) {
		super(TreeModelEncoderModelMapper::new, params);
	}

	public TreeModelEncoderStreamOp(BatchOperator model) {
		this(model, null);
	}

	public TreeModelEncoderStreamOp(BatchOperator model, Params params) {
		super(model, TreeModelEncoderModelMapper::new, params);
	}

}
