package com.alibaba.alink.operator.batch.source;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.HiveDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.HiveSourceParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;


@IoOpAnnotation(name = "hive_batch_source", ioType = IOType.SourceBatch)
public final class HiveSourceBatchOp extends BaseSourceBatchOp<HiveSourceBatchOp>
    implements HiveSourceParams<HiveSourceBatchOp> {

    public HiveSourceBatchOp() {
        this(new Params());
    }

    public HiveSourceBatchOp(Params params) {
        super(AnnotationUtils.annotatedName(HiveDB.class), params);
    }

    @Override
    protected Table initializeDataSource() {
        try {
            BaseDB db = BaseDB.of(super.getParams());
            return new DBSourceBatchOp(db, getInputTableName(), super.getParams()).initializeDataSource();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }
}
