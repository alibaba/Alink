package com.alibaba.alink.common.sql.builtin.time;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 返回当前的时间戳，或加一个偏移。
 */

public class Now extends ScalarFunction {

	private static final long serialVersionUID = 3848110218939438137L;

	@Override
	public boolean isDeterministic() {
		return false;
	}

	public long eval(int offset) {
		return System.currentTimeMillis() / 1000 + offset;
	}

	public long eval() {
		return System.currentTimeMillis() / 1000;
	}
}
