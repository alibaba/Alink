package com.alibaba.alink.params.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Parameters for the group by operator.
 *
 * @param <T> The class of the group by operator.
 */
public interface GroupByParams<T> extends WithParams<T> {

    ParamInfo<String> GROUP_BY_PREDICATE = ParamInfoFactory
        .createParamInfo("groupByPredicate", String.class)
        .setDescription("Group by clause.")
        .setRequired()
        .setAlias(new String[]{"groupByClause"})
        .build();

    ParamInfo<String> SELECT_CLAUSE = ParamInfoFactory
        .createParamInfo("selectClause", String.class)
        .setDescription("Select clause")
        .setRequired()
        .build();

    default String getGroupByPredicate() {
        return get(GROUP_BY_PREDICATE);
    }

    default T setGroupByPredicate(String value) {
        return set(GROUP_BY_PREDICATE, value);
    }

    default String getSelectClause() {
        return get(SELECT_CLAUSE);
    }

    default T setSelectClause(String value) {
        return set(SELECT_CLAUSE, value);
    }
}
