package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.StopWordsRemoverMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.nlp.StopWordsRemoverParams;

/**
 * Filter stop words in a document.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("停用词过滤")
public final class StopWordsRemoverLocalOp extends MapLocalOp <StopWordsRemoverLocalOp>
	implements StopWordsRemoverParams <StopWordsRemoverLocalOp> {

	public StopWordsRemoverLocalOp() {
		this(new Params());
	}

	public StopWordsRemoverLocalOp(Params params) {
		super(StopWordsRemoverMapper::new, params);
	}
}
