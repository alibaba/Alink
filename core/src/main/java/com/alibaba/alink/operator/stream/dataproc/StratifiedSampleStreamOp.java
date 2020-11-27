package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.StratifiedSampleMapper;
import com.alibaba.alink.operator.stream.utils.FlatMapStreamOp;
import com.alibaba.alink.params.dataproc.StratifiedSampleParams;

/**
 * Stratified sample with given ratios without replacement.
 */
public final class StratifiedSampleStreamOp extends FlatMapStreamOp <StratifiedSampleStreamOp>
	implements StratifiedSampleParams <StratifiedSampleStreamOp> {

	private static final long serialVersionUID = -2662026789274113190L;

	public StratifiedSampleStreamOp() {
		this(null);
	}

	public StratifiedSampleStreamOp(Params params) {
		super(StratifiedSampleMapper::new, params);
	}
}
