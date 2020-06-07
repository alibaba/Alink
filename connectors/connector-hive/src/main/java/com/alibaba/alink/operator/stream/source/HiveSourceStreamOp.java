package com.alibaba.alink.operator.stream.source;

import com.alibaba.alink.common.io.BaseDB;
import com.alibaba.alink.common.io.HiveDB;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.HiveSourceParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;


@IoOpAnnotation(name = "hive_stream_source", ioType = IOType.SourceStream)
public final class HiveSourceStreamOp extends BaseSourceStreamOp<HiveSourceStreamOp>
    implements HiveSourceParams<HiveSourceStreamOp> {

    public HiveSourceStreamOp() {
        this(new Params());
    }

    public HiveSourceStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(HiveDB.class), params);
    }

    @Override
    protected Table initializeDataSource() {
        try {
            BaseDB db = BaseDB.of(super.getParams());
            return new DBSourceStreamOp(db, getInputTableName(), super.getParams()).initializeDataSource();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
