package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.FirstNParams;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * BatchOperator to select first n records.
 */
public class FirstNBatchOp extends BatchOperator<FirstNBatchOp>
    implements FirstNParams<FirstNBatchOp> {

    public FirstNBatchOp() {
        this(new Params());
    }

    public FirstNBatchOp(Params params) {
        super(params);
    }

    public FirstNBatchOp(int n) {
        this(new Params().set(SIZE, n));
    }

    @Override
    public FirstNBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        int n = getSize();
        DataSet<Row> rows = in.getDataSet().first(n);
        this.setOutput(rows, in.getSchema());
        return this;
    }

}
