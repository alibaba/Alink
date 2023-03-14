package com.alibaba.alink.operator.stream.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.dataproc.vector.VectorBiFunctionMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.vector.VectorBiFunctionParams;

/**
 * Vector operator with two input vectors.
 * Vector can be sparse vector or dense vector.
 */
@NameCn("二元向量函数")
@NameEn("Vector bi-function")
public final class VectorBiFunctionStreamOp extends MapStreamOp <VectorBiFunctionStreamOp>
	implements VectorBiFunctionParams <VectorBiFunctionStreamOp> {

	private static final long serialVersionUID = 6183736542103372491L;

	public VectorBiFunctionStreamOp() {
		this(null);
	}

	public VectorBiFunctionStreamOp(Params param) {
		super(VectorBiFunctionMapper::new, param);
	}

}
