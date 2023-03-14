package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * Trait for parameter sparseVectorColName.
 */
public interface HasSparseVectorCol<T> extends WithParams <T> {

	@NameCn("稀疏向量列名")
	@DescCn("稀疏向量列对应的列名")
	ParamInfo <String> SPARSE_VECTOR_COL = ParamInfoFactory
		.createParamInfo("sparseVectorCol", String.class)
		.setDescription("Name of a sparse vector column")
		.setRequired()
		.build();

	default String getSparseVectorCol() {
		return get(SPARSE_VECTOR_COL);
	}

	default T setSparseVectorCol(String colName) {
		return set(SPARSE_VECTOR_COL, colName);
	}
}
