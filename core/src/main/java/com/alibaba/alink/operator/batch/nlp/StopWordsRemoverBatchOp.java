package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.nlp.StopWordsRemoverMapper;
import com.alibaba.alink.params.nlp.StopWordsRemoverParams;

/**
 * Filter stop words in a document.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("停用词过滤")
@NameEn("StopWordsRemover")
public final class StopWordsRemoverBatchOp extends MapBatchOp <StopWordsRemoverBatchOp>
	implements StopWordsRemoverParams <StopWordsRemoverBatchOp> {

	private static final long serialVersionUID = -1707975519454792211L;

	public StopWordsRemoverBatchOp() {
		this(new Params());
	}

	public StopWordsRemoverBatchOp(Params params) {
		super(StopWordsRemoverMapper::new, params);
	}
}
