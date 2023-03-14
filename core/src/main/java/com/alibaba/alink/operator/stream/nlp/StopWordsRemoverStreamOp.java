package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.StopWordsRemoverMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.nlp.StopWordsRemoverParams;

/**
 * Filter stop words in a document.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("停用词过滤")
@NameEn("StopWords remover")
public final class StopWordsRemoverStreamOp extends MapStreamOp <StopWordsRemoverStreamOp>
	implements StopWordsRemoverParams <StopWordsRemoverStreamOp> {

	private static final long serialVersionUID = 7115103812603873196L;

	public StopWordsRemoverStreamOp() {
		this(null);
	}

	public StopWordsRemoverStreamOp(Params params) {
		super(StopWordsRemoverMapper::new, params);
	}
}
