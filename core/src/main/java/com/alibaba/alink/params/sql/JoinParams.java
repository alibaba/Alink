package com.alibaba.alink.params.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

/**
 * Parameters for the join operator.
 *
 * @param <T> The class of the join operator.
 */
public interface JoinParams<T> extends WithParams <T> {

	ParamInfo <String> JOIN_PREDICATE = ParamInfoFactory
		.createParamInfo("joinPredicate", String.class)
		.setDescription("joinPredicate")
		.setAlias(new String[] {"whereClause"})
		.setRequired()
		.build();

	ParamInfo <String> SELECT_CLAUSE = ParamInfoFactory
		.createParamInfo("selectClause", String.class)
		.setDescription("Select clause")
		.setRequired()
		.build();

	ParamInfo <Type> TYPE = ParamInfoFactory
		.createParamInfo("type", Type.class)
		.setDescription("Join type, one of \"join\", \"leftOuterJoin\", \"rightOuterJoin\", \"fullOuterJoin\"")
		.setHasDefaultValue(Type.JOIN)
		.build();

	default Type getType() {
		return get(TYPE);
	}

	default T setType(Type value) {
		return set(TYPE, value);
	}

	default T setType(String value) {
		return set(TYPE, ParamUtil.searchEnum(TYPE, value));
	}

	enum Type {
		JOIN,
		LEFTOUTERJOIN,
		RIGHTOUTERJOIN,
		FULLOUTERJOIN
	}

	default String getJoinPredicate() {
		return get(JOIN_PREDICATE);
	}

	default T setJoinPredicate(String value) {
		return set(JOIN_PREDICATE, value);
	}

	default String getSelectClause() {
		return get(SELECT_CLAUSE);
	}

	default T setSelectClause(String value) {
		return set(SELECT_CLAUSE, value);
	}

}
