package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.StringIndexerTrainBatchOp;
import com.alibaba.alink.params.dataproc.StringIndexerPredictParams;
import com.alibaba.alink.params.dataproc.StringIndexerTrainParams;
import com.alibaba.alink.pipeline.Trainer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Encode one column of strings to bigint type indices.
 * The indices are consecutive bigint type that start from 0.
 * Non-string columns are first converted to strings and then encoded.
 * <p>
 * Several string order type is supported, including:
 * <ol>
 * <li>random</li>
 * <li>frequency_asc</li>
 * <li>frequency_desc</li>
 * <li>alphabet_asc</li>
 * <li>alphabet_desc</li>
 * </ol>
 */
public class StringIndexer extends Trainer<StringIndexer, StringIndexerModel> implements
    StringIndexerTrainParams<StringIndexer>,
    StringIndexerPredictParams<StringIndexer> {


    public StringIndexer() {
        super();
    }

    public StringIndexer(Params params) {
        super(params);
    }

    @Override
    protected BatchOperator train(BatchOperator in) {
        return new StringIndexerTrainBatchOp(this.getParams()).linkFrom(in);
    }
}