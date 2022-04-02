package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.vector.VectorSliceMapper;
import com.alibaba.alink.params.dataproc.vector.VectorSliceParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the
 * original features. It is useful for extracting features from a vector column.
 */
@NameCn("向量切片")
public class VectorSlicer extends MapTransformer <VectorSlicer>
	implements VectorSliceParams <VectorSlicer> {

	private static final long serialVersionUID = 790328000908916758L;

	public VectorSlicer() {
		this(null);
	}

	public VectorSlicer(Params params) {
		super(VectorSliceMapper::new, params);
	}
}
