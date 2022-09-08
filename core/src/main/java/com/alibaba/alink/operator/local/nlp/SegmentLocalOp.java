package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.SegmentMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.nlp.SegmentParams;

/**
 * Segment Chinese document into words.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("分词")
public final class SegmentLocalOp extends MapLocalOp <SegmentLocalOp>
	implements SegmentParams <SegmentLocalOp> {

	public SegmentLocalOp() {
		this(null);
	}

	public SegmentLocalOp(Params params) {
		super(SegmentMapper::new, params);
	}
}





