package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.io.HasSchemaStr;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import java.io.Serializable;

public interface StringToColumnsParams<T> extends
    HasSelectedCol<T>,
    HasSchemaStr<T>,
    HasReservedCols<T> {

    ParamInfo<HandleInvalid> HANDLE_INVALID = ParamInfoFactory
        .createParamInfo("handleInvalid", HandleInvalid.class)
        .setDescription("handle invalid strategy")
        .setHasDefaultValue(HandleInvalid.SKIP)
        .build();

    default HandleInvalid getHandleInvalid() {
        return get(HANDLE_INVALID);
    }

    default T setHandleInvalid(HandleInvalid value) {
        return set(HANDLE_INVALID, value);
    }

    default T setHandleInvalid(String value) {
        return set(HANDLE_INVALID, ParamUtil.searchEnum(HANDLE_INVALID, value));
    }

    /**
     * Strategy to handle parse exception.
     */
    enum HandleInvalid implements Serializable {
        /**
         * Raise exception.
         */
        ERROR,
        /**
         * Pad with null.
         */
        SKIP
    }
}