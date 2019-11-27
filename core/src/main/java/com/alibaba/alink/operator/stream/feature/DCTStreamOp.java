package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.DCTMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.DCTParams;

/**
 * Discrete Cosine Transform(DCT) transforms a real-valued sequence in the time domain into another real-valued sequence
 * with same length in the frequency domain.
 */
public class DCTStreamOp extends MapStreamOp <DCTStreamOp>
	implements DCTParams <DCTStreamOp> {

	public DCTStreamOp() {
		this(null);
	}

	public DCTStreamOp(Params params) {
		super(DCTMapper::new, params);
	}
}
