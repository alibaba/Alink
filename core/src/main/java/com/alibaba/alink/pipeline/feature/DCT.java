package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.DCTMapper;
import com.alibaba.alink.params.feature.DCTParams;
import com.alibaba.alink.pipeline.MapTransformer;

public class DCT extends MapTransformer<DCT>
	implements DCTParams<DCT> {

	public DCT() {
		this(new Params());
	}

	public DCT(Params params) {
		super(DCTMapper::new, params);
	}
}
