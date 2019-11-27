package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.SegmentMapper;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.params.nlp.SegmentParams;

/**
 * Segment Chinese document into words.
 */
public final class SegmentBatchOp extends MapBatchOp <SegmentBatchOp>
	implements SegmentParams <SegmentBatchOp> {

	public SegmentBatchOp() {
		this(null);
	}

	public SegmentBatchOp(Params params) {
		super(SegmentMapper::new, params);
	}
}





