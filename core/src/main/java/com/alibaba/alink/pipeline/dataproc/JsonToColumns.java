package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.common.dataproc.StringToColumnsMappers;
import com.alibaba.alink.params.dataproc.JsonToColumnsParams;
import com.alibaba.alink.pipeline.MapTransformer;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * JsonToColumns parses a json string column to several columns,
 * according to the specified schema.
 */
public class JsonToColumns extends MapTransformer<JsonToColumns>
    implements JsonToColumnsParams<JsonToColumns> {

    public JsonToColumns() {
        this(null);
    }

    public JsonToColumns(Params params) {
        super(StringToColumnsMappers.JsonToColumnsMapper::new, params);
    }

}
