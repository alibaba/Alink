package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasGroupColDefaultAsNull;

public interface RankingListParams<T> extends HasGroupColDefaultAsNull <T> {
	@NameCn("主体列")
	@DescCn("主体列")
	ParamInfo <String> OBJECT_COL = ParamInfoFactory
		.createParamInfo("objectCol", String.class)
		.setDescription("object col name")
		.setAlias(new String[] {"objectColName"})
		.setRequired()
		.setAlias(new String[] {"objectCol"})
		.build();

	@NameCn("计算分组")
	@DescCn("计算分组, 分组列选择时必选, 用逗号分隔")
	ParamInfo <String[]> GROUP_VALUES = ParamInfoFactory
		.createParamInfo("groupValues", String[].class)
		.setDescription("group values then will has visualization")
		.setHasDefaultValue(null)
		.build();

	@NameCn("计算列")
	@DescCn("计算列")
	ParamInfo <String> STAT_COL = ParamInfoFactory
		.createParamInfo("statCol", String.class)
		.setDescription("stat col of ranking")
		.setAlias(new String[] {"statColName"})
		.setHasDefaultValue(null)
		.setAlias(new String[] {"statCol"})
		.build();

	@NameCn("统计类型")
	@DescCn("统计类型")
	ParamInfo <StatType> STAT_TYPE = ParamInfoFactory
		.createParamInfo("statType", StatType.class)
		.setDescription("stat type: count, min, max, mean,variance, countTotal")
		.setHasDefaultValue(StatType.count)
		.build();

	@NameCn("附加列")
	@DescCn("附加列")
	ParamInfo <String[]> ADDED_COLS = ParamInfoFactory
		.createParamInfo("addedCols", String[].class)
		.setDescription("Additional columns.")
		.setAlias(new String[] {"addedColNames"})
		.setHasDefaultValue(null)
		.setAlias(new String[] {"addedCols"})
		.build();

	@NameCn("附加列统计类型")
	@DescCn("附加列统计类型")
	ParamInfo <String[]> ADDED_STAT_TYPES = ParamInfoFactory
		.createParamInfo("addedStatTypes", String[].class)
		.setDescription("Additional columns stat type.")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"addedStatType"})
		.build();

	@NameCn("是否降序")
	@DescCn("是否降序")
	ParamInfo <Boolean> IS_DESCENDING = ParamInfoFactory
		.createParamInfo("isDescending", Boolean.class)
		.setDescription("is descending.")
		.setHasDefaultValue(false)
		.build();

	@NameCn("个数")
	@DescCn("个数")
	ParamInfo <Integer> TOP_N = ParamInfoFactory
		.createParamInfo("topN", Integer.class)
		.setDescription("top n")
		.setHasDefaultValue(10)
		.build();

	default String getObjectCol() {
		return get(OBJECT_COL);
	}

	default T setObjectCol(String value) {
		return set(OBJECT_COL, value);
	}

	default String[] getGroupValues() {
		return get(GROUP_VALUES);
	}

	default T setGroupValues(String[] value) {
		return set(GROUP_VALUES, value);
	}

	default String getStatCol() {
		return get(STAT_COL);
	}

	default T setStatCol(String value) {
		return set(STAT_COL, value);
	}

	default StatType getStatType() {
		return get(STAT_TYPE);
	}

	default T setStatType(StatType value) {
		return set(STAT_TYPE, value);
	}

	default T setStatType(String value) {
		return set(STAT_TYPE, ParamUtil.searchEnum(STAT_TYPE, value));
	}

	default String[] getAddedCols() {
		return get(ADDED_COLS);
	}

	default T setAddedCols(String[] value) {
		return set(ADDED_COLS, value);
	}

	default String[] getAddedStatTypes() {
		return get(ADDED_STAT_TYPES);
	}

	default T setAddedStatTypes(String[] value) {
		return set(ADDED_STAT_TYPES, value);
	}

	default Boolean getIsDescending() {
		return get(IS_DESCENDING);
	}

	default T setIsDescending(Boolean value) {
		return set(IS_DESCENDING, value);
	}

	default Integer getTopN() {
		return get(TOP_N);
	}

	default T setTopN(Integer value) {
		return set(TOP_N, value);
	}

	enum StatType {
		count,
		countTotal,
		min,
		max,
		sum,
		mean,
		variance
	}
}
