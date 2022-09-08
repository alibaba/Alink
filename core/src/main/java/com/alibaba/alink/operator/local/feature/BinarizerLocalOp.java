package com.alibaba.alink.operator.local.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.feature.BinarizerMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.feature.BinarizerParams;

/**
 * Binarize a continuous variable using a threshold.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@NameCn("二值化")
public final class BinarizerLocalOp extends MapLocalOp <BinarizerLocalOp>
	implements BinarizerParams <BinarizerLocalOp> {

	public BinarizerLocalOp() {
		this(null);
	}

	public BinarizerLocalOp(Params params) {
		super(BinarizerMapper::new, params);
	}
}
