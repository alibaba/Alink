package com.alibaba.alink.operator.batch.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the statistics provided by DocHashCountVectorizerTrainBatchOp.
 * It uses MurmurHash 3 to get the hash value of a word as the index.
 */
public class DocHashCountVectorizerPredictBatchOp extends ModelMapBatchOp <DocHashCountVectorizerPredictBatchOp>
	implements DocHashCountVectorizerPredictParams<DocHashCountVectorizerPredictBatchOp> {
	public DocHashCountVectorizerPredictBatchOp() {
		this(new Params());
	}

	public DocHashCountVectorizerPredictBatchOp(Params params) {
		super(DocHashCountVectorizerModelMapper::new, params);
	}
}
