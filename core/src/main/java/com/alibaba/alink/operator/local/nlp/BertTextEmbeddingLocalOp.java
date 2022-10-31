package com.alibaba.alink.operator.local.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.bert.BertTextEmbeddingMapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.tensorflow.bert.BertTextEmbeddingParams;

/**
 * This operator extracts embeddings of sentences by feeding them to a BERT model.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("Bert文本嵌入")
public class BertTextEmbeddingLocalOp extends MapLocalOp <BertTextEmbeddingLocalOp>
	implements BertTextEmbeddingParams <BertTextEmbeddingLocalOp> {

	public BertTextEmbeddingLocalOp() {
		this(new Params());
	}

	public BertTextEmbeddingLocalOp(Params params) {
		super(BertTextEmbeddingMapper::new, params);
	}
}
