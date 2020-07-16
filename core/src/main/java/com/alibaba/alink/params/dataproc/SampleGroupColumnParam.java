package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface SampleGroupColumnParam<T> extends
        WithParams<T>  {

    ParamInfo<String> GROUP_COL = ParamInfoFactory
            .createParamInfo("groupCol", String.class)
            .setDescription("name of group column")
            .setRequired()
            .build();
    default String getGroupCol() {
        return getParams().get(GROUP_COL);
    }

    default T setGroupCol(String value) {
        return set(GROUP_COL, value);
    }

    default int computeGroupCloIndex(String[] schema, String groupClo) {
        for (int i = 0; i < schema.length; i++) {
            if (groupClo.equals(schema[i])){
                return i;
            }
        }
        throw new RuntimeException("unknown column name :" + groupClo);
    }
}
