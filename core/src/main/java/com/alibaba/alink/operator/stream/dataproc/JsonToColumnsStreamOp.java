package com.alibaba.alink.operator.stream.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.JsonToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * JsonToColumnsStreamOp parses a json string column to several columns,
 * according to the specified schema.
 */
public final class JsonToColumnsStreamOp extends MapStreamOp<JsonToColumnsStreamOp>
    implements JsonToColumnsParams<JsonToColumnsStreamOp> {

    public JsonToColumnsStreamOp() {
        this(null);
    }

    public JsonToColumnsStreamOp(Params params) {
        super(StringToColumnsMappers.JsonToColumnsMapper::new, params);
    }

}
