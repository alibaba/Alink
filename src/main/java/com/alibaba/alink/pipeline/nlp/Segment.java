package com.alibaba.alink.pipeline.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.SegmentMapper;
import com.alibaba.alink.params.nlp.SegmentParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Segment Chinese document into words.
 */
public class Segment extends MapTransformer<Segment>
	implements SegmentParams <Segment> {

	public Segment() {
		this(null);
	}

	public Segment(Params params) {
		super(SegmentMapper::new, params);
	}
}
