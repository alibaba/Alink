package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.params.dataproc.KvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * KvToColumnsBatchOp parses a key-value string column to several columns,
 * according to the specified schema.
 * <p>
 * The key-value string has format like: f1=val1,f2=val2,f3=val3
 */
public final class KvToColumnsBatchOp extends MapBatchOp<KvToColumnsBatchOp>
    implements KvToColumnsParams<KvToColumnsBatchOp> {

    public KvToColumnsBatchOp() {
        this(null);
    }

    public KvToColumnsBatchOp(Params params) {
        super(StringToColumnsMappers.KvToColumnsMapper::new, params);
    }
}
