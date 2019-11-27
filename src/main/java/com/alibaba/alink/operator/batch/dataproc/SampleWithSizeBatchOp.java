package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.SampleWithSizeParams;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * Sample with given size with or without replacement.
 */
public class SampleWithSizeBatchOp extends BatchOperator<SampleWithSizeBatchOp>
    implements SampleWithSizeParams<SampleWithSizeBatchOp> {

    public SampleWithSizeBatchOp() {
        this(new Params());
    }

    public SampleWithSizeBatchOp(Params params) {
        super(params);
    }

    public SampleWithSizeBatchOp(int numSamples) {
        this(numSamples, false);
    }

    public SampleWithSizeBatchOp(int numSamples, boolean withReplacement) {
        this(new Params()
            .set(SIZE, numSamples)
            .set(WITH_REPLACEMENT, withReplacement)
        );
    }

    @Override
    public SampleWithSizeBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        boolean withReplacement = getWithReplacement();
        int numSamples = getSize();
        DataSet<Row> rows = DataSetUtils.sampleWithSize(in.getDataSet(), withReplacement, numSamples);
        this.setOutput(rows, in.getSchema());
        return this;
    }

}
