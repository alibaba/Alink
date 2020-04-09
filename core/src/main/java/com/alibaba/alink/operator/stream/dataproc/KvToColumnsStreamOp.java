package com.alibaba.alink.operator.stream.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.KvToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * KvToColumnsStreamOp parses a key-value string column to several columns,
 * according to the specified schema.
 * <p>
 * The key-value string has format like: f1=val1,f2=val2,f3=val3
 */
public final class KvToColumnsStreamOp extends MapStreamOp<KvToColumnsStreamOp>
    implements KvToColumnsParams<KvToColumnsStreamOp> {

    public KvToColumnsStreamOp() {
        this(null);
    }

    public KvToColumnsStreamOp(Params params) {
        super(StringToColumnsMappers.KvToColumnsMapper::new, params);
    }

}
