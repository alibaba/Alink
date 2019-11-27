package com.alibaba.alink.operator.batch.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.HasIoName;
import com.alibaba.alink.params.io.HasIoType;

/**
 * The base class of all data sources.
 *
 * @param <T>
 */

public abstract class BaseSourceBatchOp<T extends BaseSourceBatchOp<T>> extends BatchOperator<T> {

    static final IOType IO_TYPE = IOType.SourceBatch;

    protected BaseSourceBatchOp(String nameSrcSnk, Params params) {
        super(params);
        this.getParams().set(HasIoType.IO_TYPE, IO_TYPE)
            .set(HasIoName.IO_NAME, nameSrcSnk);

    }

    public static BaseSourceBatchOp of(Params params) throws Exception {
        if (params.contains(HasIoType.IO_TYPE)
            && params.get(HasIoType.IO_TYPE).equals(IO_TYPE)
            && params.contains(HasIoName.IO_NAME)) {
            if (BaseDB.isDB(params)) {
                return new DBSourceBatchOp(BaseDB.of(params), params);
            } else if (params.contains(HasIoName.IO_NAME)) {
                String name = params.get(HasIoName.IO_NAME);
                return (BaseSourceBatchOp) AnnotationUtils.createOp(name, IO_TYPE, params);
            }
        }
        throw new RuntimeException("Parameter Error.");
    }

    @Override
    public T linkFrom(BatchOperator<?>... inputs) {
        throw new UnsupportedOperationException("Source operator does not support linkFrom()");
    }

    @Override
    public Table getOutputTable() {
        if (super.getOutputTable() == null) {
            super.setOutputTable(initializeDataSource());
        }
        return super.getOutputTable();
    }

    /**
     * Initialize the table.
     */
    protected abstract Table initializeDataSource();
}
