package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.nlp.DocCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the statistics provided by DocCountVectorizerTrainBatchOp.
 * It supports several types: IDF/TF/TF-IDF/One-Hot/WordCount.
 */
public final class DocCountVectorizerPredictBatchOp extends ModelMapBatchOp <DocCountVectorizerPredictBatchOp>
	implements DocCountVectorizerPredictParams <DocCountVectorizerPredictBatchOp> {
	public DocCountVectorizerPredictBatchOp() {
		this(null);
	}

	public DocCountVectorizerPredictBatchOp(Params params) {
		super(DocCountVectorizerModelMapper::new, params);
	}
}