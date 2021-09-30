package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.nlp.bert.BertTextEmbeddingMapper;
import com.alibaba.alink.params.tensorflow.bert.BertTextEmbeddingParams;

/**
 * This operator extracts embeddings of sentences by feeding them to a BERT model.
 * <p>
 * TODO: support batch inference and intra op parallelism.
 */
public class BertTextEmbeddingBatchOp extends MapBatchOp <BertTextEmbeddingBatchOp>
	implements BertTextEmbeddingParams <BertTextEmbeddingBatchOp> {

	public BertTextEmbeddingBatchOp() {
		this(new Params());
	}

	public BertTextEmbeddingBatchOp(Params params) {
		super(BertTextEmbeddingMapper::new, params);
	}
}
