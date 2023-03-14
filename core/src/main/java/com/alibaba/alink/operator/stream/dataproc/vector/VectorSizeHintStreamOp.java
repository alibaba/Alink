package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSizeHintMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;

/**
 * Check the size of a vector. if size is not match, then do as handleInvalid
 */
@ParamSelectColumnSpec(name = "selectedCol", portIndices = 0, allowedTypeCollections = {TypeCollections.VECTOR_TYPES})
@NameCn("向量长度检验")
@NameEn("Vector size hint")
public final class VectorSizeHintStreamOp extends MapStreamOp <VectorSizeHintStreamOp>
	implements VectorSizeHintParams <VectorSizeHintStreamOp> {

	private static final long serialVersionUID = -1028181022845803834L;

	/**
	 * handleInvalidMethod can be "error", "skip", "optimistic"
	 */
	public VectorSizeHintStreamOp() {
		this(null);
	}

	public VectorSizeHintStreamOp(Params params) {
		super(VectorSizeHintMapper::new, params);
	}
}
