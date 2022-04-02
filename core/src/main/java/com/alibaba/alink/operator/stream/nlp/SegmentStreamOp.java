package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.SegmentMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.nlp.SegmentParams;

/**
 * Segment Chinese document into words.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("分词")
public final class SegmentStreamOp extends MapStreamOp <SegmentStreamOp>
	implements SegmentParams <SegmentStreamOp> {

	private static final long serialVersionUID = 1270248884300654255L;

	public SegmentStreamOp() {
		this(null);
	}

	public SegmentStreamOp(Params params) {
		super(SegmentMapper::new, params);
	}

}






