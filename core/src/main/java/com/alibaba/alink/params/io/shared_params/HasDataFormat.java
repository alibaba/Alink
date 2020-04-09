package com.alibaba.alink.params.io.shared_params;

import com.alibaba.alink.params.ParamUtil;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import java.io.Serializable;

public interface HasDataFormat<T> extends WithParams<T> {
    ParamInfo<DataFormat> DATA_FORMAT = ParamInfoFactory
        .createParamInfo("dataFormat", DataFormat.class)
        .setDescription("data format")
        .setRequired()
        .setAlias(new String[]{"type"})
        .build();

    default DataFormat getDataFormat() {
        return get(DATA_FORMAT);
    }

    default T setDataFormat(DataFormat value) {
        return set(DATA_FORMAT, value);
    }

    default T setDataFormat(String value) {
        return set(DATA_FORMAT, ParamUtil.searchEnum(DATA_FORMAT, value));
    }

    enum DataFormat implements Serializable {
        JSON, CSV
    }
}
