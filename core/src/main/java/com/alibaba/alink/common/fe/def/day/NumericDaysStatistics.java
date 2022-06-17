package com.alibaba.alink.common.fe.def.day;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.fe.def.statistics.BaseNumericStatistics;

public enum NumericDaysStatistics implements BaseNumericStatistics {
	TOTAL_COUNT(Types.LONG),
	COUNT(Types.LONG),
	SUM(Types.DOUBLE),
	MIN,
	MAX,
	MEAN(Types.DOUBLE);

	private TypeInformation <?> outType;

	NumericDaysStatistics() {
		this(null);
	}

	NumericDaysStatistics(TypeInformation <?> outType) {
		this.outType = outType;
	}

	public TypeInformation <?> getOutType() {
		return outType;
	}
}
