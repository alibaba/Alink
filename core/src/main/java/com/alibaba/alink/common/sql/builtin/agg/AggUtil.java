package com.alibaba.alink.common.sql.builtin.agg;

public class AggUtil {

	public static<T> boolean judgeNull(T a, T b, JudgeEqual judgeEqual) {
		if (a == null && b == null) {
			return true;
		}
		if (a == null || b == null) {
			return false;
		}
		return judgeEqual.judgeEqual(a, b);
	}

	@FunctionalInterface
	public interface JudgeEqual<IN> {
		public boolean judgeEqual(IN a, IN b);
	}

	public static boolean simpleJudgeEqual(Object a, Object b) {
		return (a).equals(b);
	}
}
