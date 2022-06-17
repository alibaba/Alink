package com.alibaba.alink.common.fe.def.day;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.fe.def.statistics.BaseCategoricalStatistics;

public enum CategoricalDaysStatistics implements BaseCategoricalStatistics {
	TOTAL_COUNT(Types.LONG),
	COUNT(Types.LONG),
	DISTINCT_COUNT(Types.LONG),
	CATES_CNT(Types.LONG),
	KV_CNT(Types.STRING),
	KV_RATIO(Types.STRING);

	private TypeInformation <?> outType;

	CategoricalDaysStatistics() {
		this(null);
	}

	CategoricalDaysStatistics(TypeInformation <?> outType) {
		this.outType = outType;
	}

	public TypeInformation <?> getOutType() {
		return outType;
	}
}

