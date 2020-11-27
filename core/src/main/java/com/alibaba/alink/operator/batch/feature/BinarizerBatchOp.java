package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.feature.BinarizerMapper;
import com.alibaba.alink.params.feature.BinarizerParams;

/**
 * Binarize a continuous variable using a threshold.
 */
public final class BinarizerBatchOp extends MapBatchOp <BinarizerBatchOp>
	implements BinarizerParams <BinarizerBatchOp> {
	private static final long serialVersionUID = -8285479274916036924L;

	public BinarizerBatchOp() {
		this(null);
	}

	public BinarizerBatchOp(Params params) {
		super(BinarizerMapper::new, params);
	}
}
