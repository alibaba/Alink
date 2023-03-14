package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.nlp.SegmentMapper;
import com.alibaba.alink.params.nlp.SegmentParams;

/**
 * Segment Chinese document into words.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("分词")
@NameEn("Segment")
public final class SegmentBatchOp extends MapBatchOp <SegmentBatchOp>
	implements SegmentParams <SegmentBatchOp> {

	private static final long serialVersionUID = 5671501529836451015L;

	public SegmentBatchOp() {
		this(null);
	}

	public SegmentBatchOp(Params params) {
		super(SegmentMapper::new, params);
	}
}





