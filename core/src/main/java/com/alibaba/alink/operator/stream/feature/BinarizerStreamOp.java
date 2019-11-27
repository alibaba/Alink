package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.feature.BinarizerMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.BinarizerParams;

/**
 * Binarize a continuous variable using a threshold.
 */
public class BinarizerStreamOp extends MapStreamOp <BinarizerStreamOp>
	implements BinarizerParams <BinarizerStreamOp> {
	public BinarizerStreamOp() {
		this(null);
	}

	public BinarizerStreamOp(Params params) {
		super(BinarizerMapper::new, params);
	}
}
