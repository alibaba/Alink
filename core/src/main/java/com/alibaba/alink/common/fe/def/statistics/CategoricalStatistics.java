package com.alibaba.alink.common.fe.def.statistics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import org.tensorflow.op.core.Rank;

public enum CategoricalStatistics implements BaseCategoricalStatistics {

	RANGE_START_TIME(Types.SQL_TIMESTAMP),
	RANGE_END_TIME(Types.SQL_TIMESTAMP),
	COUNT(Types.LONG),
	DISTINCT_COUNT(Types.LONG),
	TOTAL_COUNT(Types.LONG),
	RATIO(Types.DOUBLE),
	FREQ(Types.LONG),
	IS_EXIST(Types.BOOLEAN);

	public static BaseCategoricalStatistics LAST_TIME_N(int n) {
		return new LastTimeN(n);
	}

	public static BaseCategoricalStatistics FIRST_TIME_N(int n) {
		return new FirstTimeN(n);
	}

	public static BaseCategoricalStatistics LAST_N(int n) {
		return new LastN(n);
	}

	public static BaseCategoricalStatistics KV_STAT(int n, RankType rankType) {
		return new KvStat(n, rankType);
	}

	public static BaseCategoricalStatistics First_N(int n) {
		return new FirstN(n);
	}

	public static BaseCategoricalStatistics CONCAT_AGG(String delimiter) {return new ConcatAgg(delimiter);}

	public static class LastN extends BaseLastN implements BaseCategoricalStatistics {
		public LastN(int n) {
			this.n = n;
		}
	}

	public static class FirstN extends BaseFirstN implements BaseCategoricalStatistics {
		public FirstN(int n) {
			this.n = n;
		}
	}

	public static class LastTimeN extends BaseLastTimeN implements BaseCategoricalStatistics {
		public LastTimeN(int n) {
			this.n = n;
		}
	}

	public static class FirstTimeN extends BaseFirstTimeN implements BaseCategoricalStatistics {
		public FirstTimeN(int n) {
			this.n = n;
		}
	}

	public static class KvStat implements BaseCategoricalStatistics {
		int n;
		RankType rankType;

		@Override
		public String name() {
			StringBuilder sbd = new StringBuilder();
			sbd.append("kv_");
			sbd.append(n);
			return sbd.toString();
		}

		public KvStat(int n, RankType rankType) {
			this.n = n;
			this.rankType = rankType;
		}

		public int getN() {
			return this.n;
		}

		public RankType getRankType() {return this.rankType;}
	}

	public static class ConcatAgg implements BaseCategoricalStatistics {
		String delimiter;

		public ConcatAgg(String delimiter) {this.delimiter = delimiter;}

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

	CategoricalStatistics() {
		this(null);
	}

	CategoricalStatistics(TypeInformation <?> outType) {
		this.outType = outType;
	}

	public TypeInformation <?> getOutType() {
		return outType;
	}

	public enum RankType {
		RANK,
		DENSE_RANK
	}

}
