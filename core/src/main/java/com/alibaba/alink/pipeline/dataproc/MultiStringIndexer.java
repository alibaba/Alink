package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.params.dataproc.MultiStringIndexerPredictParams;
import com.alibaba.alink.params.dataproc.MultiStringIndexerTrainParams;
import com.alibaba.alink.pipeline.Trainer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Encode several columns of strings to bigint type indices. The indices are consecutive bigint type
 * that start from 0. Non-string columns are first converted to strings and then encoded. Each columns
 * are encoded separately.
 * <p>
 * <p>Several string order type is supported, including:
 * <ol>
 *     <li>random</li>
 *     <li>frequency_asc</li>
 *     <li>frequency_desc</li>
 *     <li>alphabet_asc</li>
 *     <li>alphabet_desc</li>
 * </ol>
 */
public class MultiStringIndexer extends Trainer<MultiStringIndexer, MultiStringIndexerModel> implements
    MultiStringIndexerTrainParams<MultiStringIndexer>,
    MultiStringIndexerPredictParams<MultiStringIndexer> {

    public MultiStringIndexer() {
        this(new Params());
    }

    public MultiStringIndexer(Params params) {
        super(params);
    }

    @Override
    protected BatchOperator train(BatchOperator in) {
        return new MultiStringIndexerTrainBatchOp(this.getParams()).linkFrom(in);
    }
}