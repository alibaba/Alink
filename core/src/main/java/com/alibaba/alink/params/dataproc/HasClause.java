package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasClause<T> extends WithParams<T> {
    ParamInfo<String> CLAUSE = ParamInfoFactory
        .createParamInfo("clause", String.class)
        .setDescription("Operation clause.")
        .setRequired()
        .setAlias(new String[]{"param", "filter"})
        .build();

    default String getClause() {
        return get(CLAUSE);
    }

    default T setClause(String value) {
        return set(CLAUSE, value);
    }
}
