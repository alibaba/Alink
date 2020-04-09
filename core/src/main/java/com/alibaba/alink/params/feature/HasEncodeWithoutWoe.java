package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasEncodeWithoutWoe<T> extends WithParams<T> {
    ParamInfo<Encode> ENCODE = ParamInfoFactory
        .createParamInfo("encode", Encode.class)
        .setDescription("encode type: INDEX, VECTOR, ASSEMBLED_VECTOR.")
        .setHasDefaultValue(Encode.ASSEMBLED_VECTOR)
        .build();

    default Encode getEncode() {
        return get(ENCODE);
    }

    default T setEncode(Encode value) {
        return set(ENCODE, value);
    }

    default T setEncode(String value) {
        return set(ENCODE, ParamUtil.searchEnum(ENCODE, value));
    }

    /**
     * Encode type for Binning.
     */
    enum Encode {

        /**
         * Output a spasevector with only one Non-zero(the index of the bin) element.
         */
        VECTOR,

        /**
         * If there are multi columns, first encode these columns as vectors, and output the assembled vector.
         */
        ASSEMBLED_VECTOR,

        /**
         * Output the index of the bin.
         */
        INDEX
    }
}
