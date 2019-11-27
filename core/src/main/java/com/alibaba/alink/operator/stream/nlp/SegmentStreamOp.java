package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.SegmentMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.nlp.SegmentParams;

/**
 * Segment Chinese document into words.
 */
public final class SegmentStreamOp extends MapStreamOp <SegmentStreamOp>
	implements SegmentParams <SegmentStreamOp> {

	public SegmentStreamOp() {
		this(null);
	}

	public SegmentStreamOp(Params params) {
		super(SegmentMapper::new, params);
	}

}






