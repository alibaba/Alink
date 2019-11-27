package com.alibaba.alink.pipeline.nlp;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.DocCountVectorizerTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.nlp.DocCountVectorizerPredictParams;
import com.alibaba.alink.params.nlp.DocCountVectorizerTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * DocCountVectorizer converts a document to a sparse vector based on the document frequency, word count or inverse
 * document
 * frequency of every word in the document.
 */
public class DocCountVectorizer extends Trainer <DocCountVectorizer, DocCountVectorizerModel>
	implements DocCountVectorizerPredictParams <DocCountVectorizer>,
	DocCountVectorizerTrainParams <DocCountVectorizer> {

	public DocCountVectorizer() {
		super();
	}

	public DocCountVectorizer(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new DocCountVectorizerTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
