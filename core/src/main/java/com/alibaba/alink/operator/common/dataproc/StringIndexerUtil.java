package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.params.dataproc.HasStringOrderTypeDefaultAsRandom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

/**
 * A utility class to assign indices to columns of strings.
 */
public class StringIndexerUtil {

	/**
	 * Assign consecutive indices to each columns of strings. The index space of each columns
	 * are independent.
	 *
	 * @param data       The data to index.
	 * @param orderType  The way to order tokens.
	 * @param startIndex Starting index.
	 * @param ignoreNull If true, null value is ignored.
	 * @return A DataSet of tuples of column index, token, and token index.
	 */
	public static DataSet <Tuple3 <Integer, String, Long>> indexTokens(
		DataSet <Row> data, HasStringOrderTypeDefaultAsRandom.StringOrderType orderType,
		final long startIndex, final boolean ignoreNull) {

		switch (orderType) {
			case RANDOM:
				return indexRandom(data, startIndex, ignoreNull);
			case FREQUENCY_ASC:
				return indexSortedByFreq(data, startIndex, ignoreNull, true);
			case FREQUENCY_DESC:
				return indexSortedByFreq(data, startIndex, ignoreNull, false);
			case ALPHABET_ASC:
				return indexSortedByAlphabet(data, startIndex, ignoreNull, true);
			case ALPHABET_DESC:
				return indexSortedByAlphabet(data, startIndex, ignoreNull, false);
			default:
				throw new AkUnsupportedOperationException("Unsupported order type " + orderType);
		}
	}

	/**
	 * Assign consecutive indices to each columns of strings randomly. The index space of each columns
	 * are independent.
	 *
	 * @param data       The data to index.
	 * @param startIndex Starting index.
	 * @param ignoreNull If true, null value is ignored.
	 * @return A DataSet of tuples of column index, token, and token index.
	 */
	public static DataSet <Tuple3 <Integer, String, Long>> indexRandom(
		DataSet <Row> data, final long startIndex, final boolean ignoreNull) {

		DataSet <Tuple2 <Integer, String>> flattened =  flattenTokens(data, ignoreNull);
		DataSet <Tuple2 <Integer, String>> distinctTokens;
		if (ignoreNull) {
			distinctTokens = flattened.groupBy(0, 1)
				.reduce(new ReduceFunction <Tuple2 <Integer, String>>() {
					private static final long serialVersionUID = 3246078624056103227L;

					@Override
					public Tuple2 <Integer, String> reduce(Tuple2 <Integer, String> value1, Tuple2 <Integer, String>
						value2)
						throws Exception {
						return value1;
					}
				})
				.name("distinct_tokens");
		} else {
			distinctTokens = flattened.groupBy(0)
				.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, String>, Tuple2 <Integer, String>>() {
					@Override
					public void reduce(Iterable <Tuple2 <Integer, String>> values,
									   Collector <Tuple2 <Integer, String>> out) throws Exception {
						boolean containNull = false;
						HashSet<String> tokenSet = new HashSet <>();
						for (Tuple2 <Integer, String> value : values) {
							if (value.f1 == null) {
								if (!containNull) {
									containNull = true;
									out.collect(value);
								}
							} else {
								if (!tokenSet.contains(value.f1)) {
									out.collect(value);
									tokenSet.add(value.f1);
								}
							}
						}
					}
				}).name("distinct_tokens");
		}
		return zipWithIndexPerColumn(distinctTokens)
			.map(new MapFunction <Tuple3 <Long, Integer, String>, Tuple3 <Integer, String, Long>>() {
				private static final long serialVersionUID = 5009352124958484740L;

				@Override
				public Tuple3 <Integer, String, Long> map(Tuple3 <Long, Integer, String> value) throws Exception {
					return Tuple3.of(value.f1, value.f2, value.f0 + startIndex);
				}
			})
			.name("assign_index");
	}

	/**
	 * Assign consecutive indices to each columns of strings, ordered by frequency of string.
	 * The index space of each columns are independent.
	 *
	 * @param data        The data to index.
	 * @param startIndex  Starting index.
	 * @param ignoreNull  If true, null value is ignored.
	 * @param isAscending If true, strings are ordered ascending by frequency.
	 * @return A DataSet of tuples of column index, token, and token index.
	 */
	public static DataSet <Tuple3 <Integer, String, Long>> indexSortedByFreq(
		DataSet <Row> data, final long startIndex, final boolean ignoreNull, final boolean isAscending) {

		return countTokens(data, ignoreNull)
			.groupBy(0)
			.sortGroup(2, isAscending ? Order.ASCENDING : Order.DESCENDING)
			.reduceGroup(new GroupReduceFunction <Tuple3 <Integer, String, Long>, Tuple3 <Integer, String, Long>>() {
				private static final long serialVersionUID = 3454314323952925197L;

				@Override
				public void reduce(Iterable <Tuple3 <Integer, String, Long>> values,
								   Collector <Tuple3 <Integer, String, Long>> out) throws Exception {
					long id = startIndex;
					for (Tuple3 <Integer, String, Long> value : values) {
						out.collect(Tuple3.of(value.f0, value.f1, id++));
					}
				}
			});
	}

	public static DataSet <Tuple3 <Integer, String, Long>> distinct(DataSet <Row> data,
																	final long startIndex,
																	final boolean ignoreNull) {
		DataSet <Tuple2 <Integer, String>> flattened =  flattenTokens(data, ignoreNull);
		DataSet <Tuple2 <Integer, String>> distinctTokens;
		if (ignoreNull) {
			distinctTokens = flattened.groupBy(0, 1)
				.reduce(new ReduceFunction <Tuple2 <Integer, String>>() {
					private static final long serialVersionUID = 3246078624056103227L;

					@Override
					public Tuple2 <Integer, String> reduce(Tuple2 <Integer, String> value1, Tuple2 <Integer, String>
						value2)
						throws Exception {
						return value1;
					}
				})
				.name("distinct_tokens");
		} else {
			distinctTokens = flattened.groupBy(0)
				.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, String>, Tuple2 <Integer, String>>() {
					@Override
					public void reduce(Iterable <Tuple2 <Integer, String>> values,
									   Collector <Tuple2 <Integer, String>> out) throws Exception {
						boolean containNull = false;
						HashSet<String> tokenSet = new HashSet <>();
						for (Tuple2 <Integer, String> value : values) {
							if (value.f1 == null) {
								if (!containNull) {
									containNull = true;
									out.collect(value);
								}
							} else {
								if (!tokenSet.contains(value.f1)) {
									out.collect(value);
									tokenSet.add(value.f1);
								}
							}
						}
					}
				}).name("distinct_tokens");
		}
		return zipWithIndexPerColumn(distinctTokens)
			.map(new MapFunction <Tuple3 <Long, Integer, String>, Tuple3 <Integer, String, Long>>() {
				private static final long serialVersionUID = 5009352124958484740L;

				@Override
				public Tuple3 <Integer, String, Long> map(Tuple3 <Long, Integer, String> value) throws Exception {
					return Tuple3.of(value.f1, value.f2, value.f0 + startIndex);
				}
			})
			.name("assign_index");
	}

	public static DataSet <Tuple3 <Integer, String, Long>> countTokens(DataSet <Row> data, final boolean ignoreNull) {
		return flattenTokens(data, ignoreNull)
			.map(new MapFunction <Tuple2 <Integer, String>, Tuple3 <Integer, String, Long>>() {
				private static final long serialVersionUID = -3620557146524640793L;

				@Override
				public Tuple3 <Integer, String, Long> map(Tuple2 <Integer, String> value) throws Exception {
					return Tuple3.of(value.f0, value.f1, 1L);
				}
			})
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction <Tuple3<Integer, String, Long>, Tuple3<Integer, String, Long>>() {
				@Override
				public void reduce(Iterable <Tuple3 <Integer, String, Long>> values,
								   Collector <Tuple3 <Integer, String, Long>> out) throws Exception {
					int columnIndex = -1;
					long nullNumber = 0;
					HashMap<String, Long> tokenNumber = new HashMap <>();
					for (Tuple3 <Integer, String, Long> value : values) {
						if (columnIndex == -1) {
							columnIndex = value.f0;
						}
						if (value.f1 == null) {
							nullNumber += value.f2;
						} else {
							long thisNumber = tokenNumber.getOrDefault(value.f1, 0L);
							tokenNumber.put(value.f1, value.f2 + thisNumber);
						}
					}
					if (nullNumber != 0) {
						out.collect(Tuple3.of(columnIndex, null, nullNumber));
					}
					for (Entry <String, Long> entry : tokenNumber.entrySet()) {
						out.collect(Tuple3.of(columnIndex, entry.getKey(), entry.getValue()));
					}
				}
			})
			//.reduce(new ReduceFunction <Tuple3 <Integer, String, Long>>() {
			//	private static final long serialVersionUID = 4178064032728207563L;
			//
			//	@Override
			//	public Tuple3 <Integer, String, Long> reduce(Tuple3 <Integer, String, Long> value1,
			//												 Tuple3 <Integer, String, Long> value2) throws Exception {
			//		value1.f2 += value2.f2;
			//		return value1;
			//	}
			//})
			.name("count_tokens");
	}

	/**
	 * Assign consecutive indices to each columns of strings, ordered alphabetically.
	 * The index space of each columns are independent.
	 *
	 * @param data        The data to index.
	 * @param startIndex  Starting index.
	 * @param ignoreNull  If true, null value is ignored.
	 * @param isAscending If true, strings are ordered ascending by frequency.
	 * @return A DataSet of tuples of column index, token, and token index.
	 */
	public static DataSet <Tuple3 <Integer, String, Long>> indexSortedByAlphabet(
		DataSet <Row> data, final long startIndex, final boolean ignoreNull, final boolean isAscending) {

		DataSet <Tuple2 <Integer, String>> distinctTokens = flattenTokens(data, ignoreNull)
			.groupBy(0, 1)
			.reduce(new ReduceFunction <Tuple2 <Integer, String>>() {
				private static final long serialVersionUID = 8562489277234571790L;

				@Override
				public Tuple2 <Integer, String> reduce(Tuple2 <Integer, String> value1, Tuple2 <Integer, String>
					value2)
					throws Exception {
					return value1;
				}
			})
			.name("distinct_tokens");

		return distinctTokens
			.groupBy(0)
			.reduceGroup(new RichGroupReduceFunction <Tuple2 <Integer, String>, Tuple3 <Integer, String, Long>>() {
				private static final long serialVersionUID = -5673388400144888098L;

				@Override
				public void reduce(Iterable <Tuple2 <Integer, String>> values,
								   Collector <Tuple3 <Integer, String, Long>> out) throws Exception {
					int col = -1;
					List <String> tokenList = new ArrayList <>();
					for (Tuple2 <Integer, String> v : values) {
						col = v.f0;
						tokenList.add(v.f1);
					}
					if (isAscending) {
						tokenList.sort(java.util.Comparator.nullsFirst(java.util.Comparator.naturalOrder()));
					} else {
						tokenList.sort(java.util.Comparator.nullsFirst(java.util.Comparator.reverseOrder()));
					}
					for (int i = 0; i < tokenList.size(); i++) {
						out.collect(Tuple3.of(col, tokenList.get(i), startIndex + i));
					}
				}
			})
			.name("assign_index");
	}

	/**
	 * Flatten the tokens.
	 *
	 * @param data       The data to index.
	 * @param ignoreNull If true, null value is ignored.
	 * @return A DataSet of tuples of column index and token.
	 */
	private static DataSet <Tuple2 <Integer, String>> flattenTokens(DataSet <Row> data, final boolean ignoreNull) {
		return data
			.flatMap(new FlatMapFunction <Row, Tuple2 <Integer, String>>() {
				private static final long serialVersionUID = 4865017597670627434L;

				@Override
				public void flatMap(Row value, Collector <Tuple2 <Integer, String>> out) throws Exception {
					for (int i = 0; i < value.getArity(); i++) {
						Object o = value.getField(i);
						if (o == null) {
							if (!ignoreNull) {
								out.collect(Tuple2.of(i, null));
							}
						} else {
							out.collect(Tuple2.of(i, String.valueOf(o)));
						}
					}
				}
			})
			.name("flatten_tokens");
	}

	/**
	 * Assign ids to all tokens. Each columns of tokens are indexed independently.
	 *
	 * @param input The input data set consisting of group index and value.
	 * @return A data set of tuple 3 consisting of token id, group index and token.
	 */
	public static DataSet <Tuple3 <Long, Integer, String>> zipWithIndexPerColumn(
		DataSet <Tuple2 <Integer, String>> input) {
		return input
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, String>, Tuple3 <Long, Integer, String>>() {
				private static final long serialVersionUID = 1297859189970595767L;

				@Override
				public void reduce(Iterable <Tuple2 <Integer, String>> iterable,
								   Collector <Tuple3 <Long, Integer, String>> collector) throws Exception {
					long index = 0L;
					for (Tuple2 <Integer, String> record : iterable) {
						collector.collect(Tuple3.of(index, record.f0, record.f1));
						index++;
					}
				}
			});
	}
}
