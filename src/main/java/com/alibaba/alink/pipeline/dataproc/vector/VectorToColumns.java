package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.vector.VectorToColumnsMapper;
import com.alibaba.alink.params.dataproc.vector.VectorToColumnsParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Transform vector to table columns. this transformer will map vector column to many columns.
 *
 */
public class VectorToColumns extends MapTransformer<VectorToColumns>
	implements VectorToColumnsParams <VectorToColumns> {

	public VectorToColumns() {
		this(null);
	}

	public VectorToColumns(Params params) {
		super(VectorToColumnsMapper::new, params);
	}

}
