package com.alibaba.alink.common.fe.def.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.AlinkTypes;

public enum NumericStatistics implements BaseNumericStatistics {

	RANGE_START_TIME(Types.SQL_TIMESTAMP),
	RANGE_END_TIME(Types.SQL_TIMESTAMP),
	COUNT(Types.LONG),
	TOTAL_COUNT(Types.LONG),
	SUM(Types.DOUBLE),
	MEAN(Types.DOUBLE),
	STDDEV_SAMP(Types.DOUBLE),
	STDDEV_POP(Types.DOUBLE),
	VAR_SAMP(Types.DOUBLE),
	VAR_POP(Types.DOUBLE),
	SKEWNESS(Types.DOUBLE),
	SQUARE_SUM(Types.DOUBLE),
	MEDIAN(Types.DOUBLE),
	MODE(Types.DOUBLE),
	FREQ(Types.LONG),
	SUMMARY(AlinkTypes.M_TABLE),
	RANK(Types.LONG),
	DENSE_RANK(Types.LONG),
	ROW_NUMBER(Types.LONG),
	IS_EXIST(Types.BOOLEAN);

	public static BaseNumericStatistics LAST_TIME_N(int n) {
		return new LastTimeN(n);
	}

	public static BaseNumericStatistics FIRST_TIME_N(int n) {
		return new FirstTimeN(n);
	}

	public static BaseNumericStatistics LAST_N(int n) {
		return new LastN(n);
	}

	public static BaseNumericStatistics First_N(int n) {
		return new FirstN(n);
	}

	public static BaseNumericStatistics SUM_LAST_N(int n) {
		return new SumLastN(n);
	}

	public static BaseNumericStatistics CONCAT_AGG(String delimiter) {
		return new ConcatAgg(delimiter);
	}

	public static class LastN extends BaseLastN implements BaseNumericStatistics {
		public LastN(int n) {
			this.n = n;
		}
	}

	public static class FirstN extends BaseFirstN implements BaseNumericStatistics {
		public FirstN(int n) {
			this.n = n;
		}
	}

	public static class LastTimeN extends BaseLastTimeN implements BaseNumericStatistics {
		public LastTimeN(int n) {
			this.n = n;
		}
	}

	public static class FirstTimeN extends BaseFirstTimeN implements BaseNumericStatistics {
		public FirstTimeN(int n) {
			this.n = n;
		}
	}

	public static class SumLastN implements BaseNumericStatistics {
		int n;
		@Override
		public String name() {
			return "sumlastn_" + n;
		}
		public int getN() {
			return n;
		}
		public SumLastN(int n) {
			this.n = n;
		}
	}
	public static class ConcatAgg implements BaseStatistics, BaseNumericStatistics{
		String delimiter;
		public ConcatAgg(String delimiter){this.delimiter = delimiter;}
		@Override
		public String name() {
			return "concat_";
		}
		public String getDelimiter() {
			return delimiter;
		}
	}
	//if outType is null, then outType is same with input.
	private final TypeInformation <?> outType;

	NumericStatistics() {
		this(null);
	}

	NumericStatistics(TypeInformation <?> outType) {
		this.outType = outType;
	}

	public TypeInformation <?> getOutType() {
		return outType;
	}
}
