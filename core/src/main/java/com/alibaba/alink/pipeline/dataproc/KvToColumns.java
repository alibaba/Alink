package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.params.dataproc.KvToColumnsParams;
import com.alibaba.alink.pipeline.MapTransformer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * KvToColumns parses a key-value string column to several columns,
 * according to the specified schema.
 * <p>
 * The key-value string has format like: f1=val1,f2=val2,f3=val3
 */
public class KvToColumns extends MapTransformer<KvToColumns>
    implements KvToColumnsParams<KvToColumns> {

    public KvToColumns() {
        this(null);
    }

    public KvToColumns(Params params) {
        super(StringToColumnsMappers.KvToColumnsMapper::new, params);
    }

}
