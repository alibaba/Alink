package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.feature.BinarizerMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.BinarizerParams;

/**
 * Binarize a continuous variable using a threshold.
 */
@ParamSelectColumnSpec(name="selectedCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("二值化")
@NameEn("Binarizer")
public class BinarizerStreamOp extends MapStreamOp <BinarizerStreamOp>
	implements BinarizerParams <BinarizerStreamOp> {
	private static final long serialVersionUID = -504377217621358550L;

	public BinarizerStreamOp() {
		this(null);
	}

	public BinarizerStreamOp(Params params) {
		super(BinarizerMapper::new, params);
	}
}
