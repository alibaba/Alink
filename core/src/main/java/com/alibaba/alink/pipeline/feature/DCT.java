package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.DCTMapper;
import com.alibaba.alink.params.feature.DCTParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Discrete Cosine Transform(DCT) transforms a real-valued sequence in the time domain into another real-valued sequence
 * with same length in the frequency domain.
 */
@NameCn("离散余弦变换")
public class DCT extends MapTransformer <DCT>
	implements DCTParams <DCT> {

	private static final long serialVersionUID = 8394824597551977174L;

	public DCT() {
		this(new Params());
	}

	public DCT(Params params) {
		super(DCTMapper::new, params);
	}
}
