package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.feature.DCTMapper;
import com.alibaba.alink.params.feature.DCTParams;

/**
 * Discrete Cosine Transform(DCT) transforms a real-valued sequence in the time domain into another real-valued sequence
 * with same length in the frequency domain.
 */
public class DCTBatchOp extends MapBatchOp <DCTBatchOp>
	implements DCTParams <DCTBatchOp> {

	private static final long serialVersionUID = 2777354116233486841L;

	public DCTBatchOp() {
		this(null);
	}

	public DCTBatchOp(Params params) {
		super(DCTMapper::new, params);
	}
}
