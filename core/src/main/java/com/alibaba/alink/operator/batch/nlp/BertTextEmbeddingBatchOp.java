package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.nlp.bert.BertTextEmbeddingMapper;
import com.alibaba.alink.params.tensorflow.bert.BertTextEmbeddingParams;

/**
 * This operator extracts embeddings of sentences by feeding them to a BERT model.
 */
@ParamSelectColumnSpec(name = "selectedCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@NameCn("Bert文本嵌入")
@NameEn("Bert Text Embedding")
public class BertTextEmbeddingBatchOp extends MapBatchOp <BertTextEmbeddingBatchOp>
	implements BertTextEmbeddingParams <BertTextEmbeddingBatchOp> {

	public BertTextEmbeddingBatchOp() {
		this(new Params());
	}

	public BertTextEmbeddingBatchOp(Params params) {
		super(BertTextEmbeddingMapper::new, params);
	}
}
