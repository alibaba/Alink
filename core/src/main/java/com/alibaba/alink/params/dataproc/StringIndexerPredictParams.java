package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Parameters for {@link com.alibaba.alink.pipeline.dataproc.StringIndexerModel}.
 */
public interface StringIndexerPredictParams<T> extends
    HasSelectedCol<T>,
    HasReservedCols<T>,
    HasOutputColDefaultAsNull<T> {

    ParamInfo<String> HANDLE_INVALID = ParamInfoFactory
        .createParamInfo("handleInvalid", String.class)
        .setDescription("Strategy to handle unseen token when doing prediction, one of \"keep\", \"skip\" or "
            + "\"error\"")
        .setHasDefaultValue("keep")
        .build();

    default String getHandleInvalid() {
        return get(HANDLE_INVALID);
    }

    default T setHandleInvalid(String value) {
        return set(HANDLE_INVALID, value);
    }
}
