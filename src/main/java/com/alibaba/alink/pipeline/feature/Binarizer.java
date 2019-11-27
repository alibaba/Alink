package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.common.feature.BinarizerMapper;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.feature.BinarizerParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Binarize a continuous variable using a threshold.
 */
public class Binarizer extends MapTransformer<Binarizer>
	implements BinarizerParams <Binarizer> {

	public Binarizer() {
		this(null);
	}

	public Binarizer(Params params) {
		super(BinarizerMapper::new, params);
	}
}
