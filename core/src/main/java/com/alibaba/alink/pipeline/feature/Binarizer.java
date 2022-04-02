package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.feature.BinarizerMapper;
import com.alibaba.alink.params.feature.BinarizerParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Binarize a continuous variable using a threshold.
 */
@NameCn("二值化")
public class Binarizer extends MapTransformer <Binarizer>
	implements BinarizerParams <Binarizer> {

	private static final long serialVersionUID = 5556074239997107381L;

	public Binarizer() {
		this(null);
	}

	public Binarizer(Params params) {
		super(BinarizerMapper::new, params);
	}
}
