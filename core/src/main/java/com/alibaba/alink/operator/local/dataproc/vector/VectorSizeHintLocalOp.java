package com.alibaba.alink.operator.local.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSizeHintMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.dataproc.vector.VectorSizeHintParams;

/**
 * Check the size of a vector. if size is not match, then do as handleInvalid.
 * If error, will throw exception if the vector is null or the vector size doesn't match the given one.
 * If optimistic, will accept the vector if it is not null.
 */
@NameCn("向量长度检验")
public final class VectorSizeHintLocalOp extends MapLocalOp <VectorSizeHintLocalOp>
	implements VectorSizeHintParams <VectorSizeHintLocalOp> {

	/**
	 * handleInvalid can be "error", "optimistic"
	 */
	public VectorSizeHintLocalOp() {
		this(null);
	}

	public VectorSizeHintLocalOp(Params params) {
		super(VectorSizeHintMapper::new, params);
	}
}
