package com.alibaba.alink.operator.batch.dataproc.format;

import com.alibaba.alink.operator.common.dataproc.format.FormatType;
import com.alibaba.alink.params.dataproc.format.JsonToTripleParams;
import org.apache.flink.ml.api.misc.param.Params;

public class JsonToTripleBatchOp extends AnyToTripleBatchOp<JsonToTripleBatchOp>
    implements JsonToTripleParams<JsonToTripleBatchOp> {

    private static final long serialVersionUID = 7543648266815893977L;

    public JsonToTripleBatchOp() {
        this(new Params());
    }

    public JsonToTripleBatchOp(Params params) {
        super(FormatType.JSON, params);
    }

}