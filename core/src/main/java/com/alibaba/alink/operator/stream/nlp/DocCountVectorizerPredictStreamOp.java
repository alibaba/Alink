package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.DocCountVectorizerModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.nlp.DocCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the statistics provided by DocCountVectorizerTrainBatchOp.
 * It supports several types: IDF/TF/TF-IDF/One-Hot/WordCount.
 * It processes streaming data.
 */
public final class DocCountVectorizerPredictStreamOp extends ModelMapStreamOp <DocCountVectorizerPredictStreamOp>
	implements DocCountVectorizerPredictParams <DocCountVectorizerPredictStreamOp> {
	public DocCountVectorizerPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public DocCountVectorizerPredictStreamOp(BatchOperator model, Params params) {
		super(model, DocCountVectorizerModelMapper::new, params);
	}
}
