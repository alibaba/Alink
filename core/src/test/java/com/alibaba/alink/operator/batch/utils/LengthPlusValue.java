package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

public class LengthPlusValue extends ScalarFunction {
	private static final long serialVersionUID = 6349759700957814180L;

	public long eval(String s, long v) {
		return s.length() + v;
	}

	@Override
	public TypeInformation <?> getResultType(Class <?>[] signature) {
		return Types.LONG;
	}
}
