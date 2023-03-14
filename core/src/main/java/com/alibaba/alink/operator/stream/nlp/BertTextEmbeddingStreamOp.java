package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.common.nlp.bert.BertTextEmbeddingMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.tensorflow.bert.BertTextEmbeddingParams;

/**
 * This operator extracts embeddings of sentences by feeding them to a BERT model.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("Bert文本嵌入")
@NameEn("Bert text embedding")
public class BertTextEmbeddingStreamOp extends MapStreamOp <BertTextEmbeddingStreamOp>
	implements BertTextEmbeddingParams <BertTextEmbeddingStreamOp> {

	public BertTextEmbeddingStreamOp() {
		this(new Params());
	}

	public BertTextEmbeddingStreamOp(Params params) {
		super(BertTextEmbeddingMapper::new, params);
	}
}
