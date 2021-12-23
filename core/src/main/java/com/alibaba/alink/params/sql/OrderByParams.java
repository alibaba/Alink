package com.alibaba.alink.params.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.dataproc.HasClause;

/**
 * Parameters for the order by operator.
 *
 * @param <T> The class of the order by operator.
 */
public interface OrderByParams<T> extends WithParams <T>, HasClause <T> {

	/**
	 * @cn-name fetch的record数目
	 * @cn fetch的record数目
	 */
	ParamInfo <Integer> FETCH = ParamInfoFactory
		.createParamInfo("fetch", Integer.class)
		.setDescription("Number of records to fetch")
		.setOptional()
		.build();
	/**
	 * @cn-name record的limit数
	 * @cn record的limit数
	 */
	ParamInfo <Integer> LIMIT = ParamInfoFactory
		.createParamInfo("limit", Integer.class)
		.setDescription("Number of records limited")
		.setOptional()
		.build();

	/**
	 * @cn-name fetch的偏移值
	 * @cn fetch的偏移值
	 */
	ParamInfo <Integer> OFFSET = ParamInfoFactory
		.createParamInfo("offset", Integer.class)
		.setDescription("Offset when fetching records")
		.setOptional()
		.build();

	default Integer getFetch() {
		return get(FETCH);
	}

	default T setFetch(Integer value) {
		return set(FETCH, value);
	}

	default Integer getLimit() {
		return get(LIMIT);
	}

	default T setLimit(Integer value) {
		return set(LIMIT, value);
	}

	default Integer getOffset() {
		return get(OFFSET);
	}

	default T setOffset(Integer value) {
		return set(OFFSET, value);
	}

	/**
	 * @cn-name 排序方法
	 * @cn 排序方法
	 */
	ParamInfo <String> ORDER = ParamInfoFactory
		.createParamInfo("order", String.class)
		.setDescription("asc or desc")
		.setHasDefaultValue("asc")
		.build();

	default String getOrder() {
		return get(ORDER);
	}

	default T setOrder(String value) {
		return set(ORDER, value);
	}
}
