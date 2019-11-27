package com.alibaba.alink.operator.stream.nlp;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.nlp.DocHashCountVectorizerModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.nlp.DocHashCountVectorizerPredictParams;

/**
 * Transform a document to a sparse vector based on the inverse document frequency(idf) statistics provided by
 * DocHashCountVectorizerTrainBatchOp. It uses MurmurHash 3 to get the hash value of a word as the index.
 */
public class DocHashCountVectorizerPredictStreamOp extends ModelMapStreamOp<DocHashCountVectorizerPredictStreamOp>
    implements DocHashCountVectorizerPredictParams<DocHashCountVectorizerPredictStreamOp> {
    public DocHashCountVectorizerPredictStreamOp(BatchOperator model) {
        this(model, new Params());
    }

    public DocHashCountVectorizerPredictStreamOp(BatchOperator model, Params params) {
        super(model, DocHashCountVectorizerModelMapper::new, params);
    }
}
