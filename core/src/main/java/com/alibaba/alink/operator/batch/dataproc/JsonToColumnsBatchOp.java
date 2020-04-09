package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.params.dataproc.JsonToColumnsParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * JsonToColumnsBatchOp parses a json string column to several columns,
 * according to the specified schema.
 */
public final class JsonToColumnsBatchOp extends MapBatchOp<JsonToColumnsBatchOp>
    implements JsonToColumnsParams<JsonToColumnsBatchOp> {

    public JsonToColumnsBatchOp() {
        this(null);
    }

    public JsonToColumnsBatchOp(Params params) {
        super(StringToColumnsMappers.JsonToColumnsMapper::new, params);
    }
}
