package com.alibaba.alink.operator.common.sql.functions;

import org.apache.flink.table.utils.EncodingUtils;

import org.apache.calcite.linq4j.tree.Types;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

public class MathFunctions implements Serializable {

	private static final long serialVersionUID = -6148088312596255496L;
	public static Method LOG2 = Types.lookupMethod(MathFunctions.class, "log2", double.class);
	public static Method LOG2_DEC = Types.lookupMethod(MathFunctions.class, "log2", BigDecimal.class);
	public static Method LOG = Types.lookupMethod(MathFunctions.class, "log", double.class);
	public static Method LOG_DEC = Types.lookupMethod(MathFunctions.class, "log", BigDecimal.class);
	public static Method LOG_WITH_BASE = Types.lookupMethod(MathFunctions.class, "log", double.class, double.class);
	public static Method LOG_WITH_BASE_DOU_DEC = Types.lookupMethod(MathFunctions.class, "log", double.class,
		BigDecimal.class);
	public static Method LOG_WITH_BASE_DEC_DOU = Types.lookupMethod(MathFunctions.class, "log", BigDecimal.class,
		double.class);
	public static Method LOG_WITH_BASE_DEC_DEC = Types.lookupMethod(MathFunctions.class, "log", BigDecimal.class,
		BigDecimal.class);
	public static Method SINH = Types.lookupMethod(MathFunctions.class, "sinh", double.class);
	public static Method SINH_DEC = Types.lookupMethod(MathFunctions.class, "sinh", BigDecimal.class);
	public static Method COSH = Types.lookupMethod(MathFunctions.class, "cosh", double.class);
	public static Method COSH_DEC = Types.lookupMethod(MathFunctions.class, "cosh", BigDecimal.class);
	public static Method TANH = Types.lookupMethod(MathFunctions.class, "tanh", double.class);
	public static Method TANH_DEC = Types.lookupMethod(MathFunctions.class, "tanh", BigDecimal.class);

	public static Method UUID = Types.lookupMethod(MathFunctions.class, "uuid");

	public static Method BIN = Types.lookupMethod(MathFunctions.class, "bin", long.class);
	public static Method HEX_LONG = Types.lookupMethod(MathFunctions.class, "hex", long.class);
	public static Method HEX_STRING = Types.lookupMethod(MathFunctions.class, "hex", String.class);

	public static double log2(double x) {
		return Math.log(x) / Math.log(2);
	}

	public static double log2(BigDecimal x) {
		return log2(x.doubleValue());
	}

	public static double log(double x) {
		return Math.log(x);
	}

	public static double log(BigDecimal x) {
		return Math.log(x.doubleValue());
	}

	public static double log(double base, double x) {
		return Math.log(x) / Math.log(base);
	}

	public static double log(double base, BigDecimal x) {
		return log(base, x.doubleValue());
	}

	public static double log(BigDecimal base, double x) {
		return log(base.doubleValue(), x);
	}

	public static double log(BigDecimal base, BigDecimal x) {
		return log(base.doubleValue(), x.doubleValue());
	}

	public static Double sinh(double x) {
		return StrictMath.sinh(x);
	}

	public static Double sinh(BigDecimal x) {
		return StrictMath.sinh(x.doubleValue());
	}

	public static Double cosh(double x) {
		return StrictMath.cosh(x);
	}

	public static Double cosh(BigDecimal x) {
		return StrictMath.cosh(x.doubleValue());
	}

	public static Double tanh(double x) {
		return StrictMath.tanh(x);
	}

	public static Double tanh(BigDecimal x) {
		return StrictMath.tanh(x.doubleValue());
	}

	public static String uuid() {
		return java.util.UUID.randomUUID().toString();
	}

	public static String bin(long x) {
		return Long.toBinaryString(x);
	}

	public static String hex(long x) {
		return Long.toHexString(x).toUpperCase();
	}

	public static String hex(String x) {
		return EncodingUtils.hex(x.getBytes(StandardCharsets.UTF_8)).toUpperCase();
	}
}
