package com.alibaba.alink.params.statistics;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasStatLevel_L1<T> extends WithParams <T> {
	
	@NameCn("统计级别")
	@DescCn(
		"将计算的统计级别。L1 has basic statistics; L2 has basic statistics and cov/corr; L3 has basic statistics, cov/corr, histogram, freq, and top/bottom k.")
	ParamInfo <StatLevel> STAT_LEVEL = ParamInfoFactory
		.createParamInfo("statLevel", StatLevel.class)
		.setDescription(
			"L1, L2, L3. L1 has basic statistics; L2 has basic statistics and cov/corr; L3 has basic statistics, "
				+ "cov/corr, histogram, freq, and top/bottom k.")
		.setHasDefaultValue(StatLevel.L1)
		.build();

	default StatLevel getStatLvel() {
		return get(STAT_LEVEL);
	}

	default T setStatLevel(StatLevel value) {
		return set(STAT_LEVEL, value);
	}

	default T setStatLevel(String value) {
		return set(STAT_LEVEL, ParamUtil.searchEnum(STAT_LEVEL, value));
	}

	enum StatLevel {
		L1,
		L2,
		L3
	}
}

