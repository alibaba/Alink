package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface MultiStringIndexerPredictParams<T> extends
    HasSelectedCols<T>,
    HasReservedCols<T>,
    HasOutputColsDefaultAsNull<T> {

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
