package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.params.dataproc.HasStringOrderTypeDefaultAsRandom;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * A utility class to assign indices to columns of strings. parallel implement
 */
public class HugeStringIndexerUtil {

	/**
	 * Assign consecutive indices to each columns of strings. The index space of each columns
	 * are independent.
	 *
	 * @param data       The data to index.
	 * @param orderType  The way to order tokens.
	 * @param startIndex Starting index.
	 * @return A DataSet of tuples of column index, token, and token index.
	 */
	public static DataSet <Tuple3 <Integer, String, Long>> indexTokens(
		DataSet <Tuple2 <Integer, String>> data, HasStringOrderTypeDefaultAsRandom.StringOrderType orderType,
		final long startIndex) {

		switch (orderType) {
			case RANDOM:
				return indexRandom(data, startIndex);
			case FREQUENCY_ASC:
				return indexSortedByFreq(data, startIndex, true);
			case FREQUENCY_DESC:
				return indexSortedByFreq(data, startIndex, false);
			case ALPHABET_ASC:
				return indexSortedByAlphabet(data, startIndex, true);
			case ALPHABET_DESC:
				return indexSortedByAlphabet(data, startIndex, false);
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
	 * @return A DataSet of tuples of column index, token, and token index.
	 */
	public static DataSet <Tuple3 <Integer, String, Long>> indexRandom(DataSet <Tuple2 <Integer, String>> data,
																	   final long startIndex) {

		DataSet <Tuple2 <Integer, String>> distinctTokens = distinct(data);
		return zipWithIndex(distinctTokens, startIndex);
	}

	/**
	 * Assign consecutive indices to each columns of strings, ordered by frequency of string.
	 * The index space of each columns are independent.
	 *
	 * @param data        The data to index.
	 * @param startIndex  Starting index.
	 * @param isAscending If true, strings are ordered ascending by frequency.
	 * @return A DataSet of tuples of column index, token, and token index.
	 */
	public static DataSet <Tuple3 <Integer, String, Long>> indexSortedByFreq(
		DataSet <Tuple2 <Integer, String>> data, final long startIndex, final boolean isAscending) {

		DataSet <Tuple3 <Integer, String, Long>> tokensWithCounts = countStringTokens(data);
		DataSet <Tuple3 <Integer, Long, Long>> counts = countLongTokens(tokensWithCounts.project(0, 2));

		return tokensWithCounts.groupBy(0, 2)
			.reduceGroup(
				new RichGroupReduceFunction <Tuple3 <Integer, String, Long>, Tuple3 <Integer, String, Long>>() {
					HashMap <Long, Long>[] mapList = null;

					public void open(Configuration parameters) throws Exception {
						List <Tuple3 <Integer, Long, Long>> list = this.getRuntimeContext().getBroadcastVariable(
							"counts");
						mapList = sortCounts(list, isAscending, startIndex);
					}

					@Override
					public void reduce(Iterable <Tuple3 <Integer, String, Long>> iterable,
									   Collector <Tuple3 <Integer, String, Long>> collector) throws Exception {
						Long id = -1L;
						for (Tuple3 <Integer, String, Long> value : iterable) {
							if (id < 0) {
								id = mapList[value.f0].getOrDefault(value.f2, startIndex);
							}
							collector.collect(Tuple3.of(value.f0, value.f1, id++));
						}
					}
				}).withBroadcastSet(counts, "counts")
			.name("assign_index_sort_by_frequency")
			.returns(new TupleTypeInfo <>(Types.INT, Types.STRING, Types.LONG));
	}

	public static HashMap <Long, Long>[] sortCounts(List <Tuple3 <Integer, Long, Long>> list, boolean isAscending,
													 long startIndex) {
		HashMap <Integer, ArrayList <Tuple2 <Long, Long>>> map = new HashMap <>();
		for (Tuple3 <Integer, Long, Long> value : list) {
			ArrayList <Tuple2 <Long, Long>> idList = map.getOrDefault(value.f0, new ArrayList <>());
			idList.add(Tuple2.of(value.f1, value.f2));
			map.put(value.f0, idList);
		}
		HashMap <Long, Long>[] mapList = new HashMap[map.size()];
		for (int i = 0; i < mapList.length; i++) {
			mapList[i] = new HashMap <>();
		}
		for (Entry <Integer, ArrayList <Tuple2 <Long, Long>>> entry : map.entrySet()) {
			ArrayList <Tuple2 <Long, Long>> entryValue = entry.getValue();
			entryValue.sort(new Comparator <Tuple2 <Long, Long>>() {
				@Override
				public int compare(Tuple2 <Long, Long> o1, Tuple2 <Long, Long> o2) {
					if (isAscending) {
						return Long.compare(o1.f0, o2.f0);
					}
					return Long.compare(o2.f0, o1.f0);
				}
			});
			long totalCount = startIndex;
			for (int i = 0; i < entryValue.size(); i++) {
				mapList[entry.getKey()].put(entryValue.get(i).f0, totalCount);
				totalCount += entryValue.get(i).f1;
			}
		}
		return mapList;
	}

	public static DataSet <Tuple2 <Integer, String>> distinct(DataSet <Tuple2 <Integer, String>> data) {
		return data.groupBy(0, 1)
			.reduce(new ReduceFunction <Tuple2 <Integer, String>>() {
				private static final long serialVersionUID = 3246078624056103227L;

				@Override
				public Tuple2 <Integer, String> reduce(Tuple2 <Integer, String> value1, Tuple2 <Integer, String>
					value2)
					throws Exception {
					return value1;
				}
			})
			.name("distinct_tokens")
			.returns(new TupleTypeInfo <>(Types.INT, Types.STRING));
	}

	public static <T> DataSet <Tuple3 <Integer, T, Long>> countTokens(DataSet <Tuple2 <Integer, T>> data, TypeInformation type) {
		return data.groupBy(0, 1)
			.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, T>, Tuple3 <Integer, T, Long>>() {
				@Override
				public void reduce(Iterable <Tuple2 <Integer, T>> values,
								   Collector <Tuple3 <Integer, T, Long>> out) throws Exception {
					int columnIndex = -1;
					T featureValue = null;
					long num = 0;
					for (Tuple2 <Integer, T> value : values) {
						if (num == 0) {
							columnIndex = value.f0;
							featureValue = value.f1;
						}
						num++;
					}
					if (num != 0) {
						out.collect(Tuple3.of(columnIndex, featureValue, num));
					}
				}
			})
			.name("count_tokens")
			.returns(new TupleTypeInfo <>(Types.INT, type, Types.LONG));
	}

	public static DataSet <Tuple3 <Integer, String, Long>> countStringTokens(DataSet <Tuple2 <Integer, String>> data) {
		return data.groupBy(0, 1)
			.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, String>, Tuple3 <Integer, String, Long>>() {
				@Override
				public void reduce(Iterable <Tuple2 <Integer, String>> values,
								   Collector <Tuple3 <Integer, String, Long>> out) throws Exception {
					int columnIndex = -1;
					String featureValue = null;
					long num = 0;
					for (Tuple2 <Integer, String> value : values) {
						if (num == 0) {
							columnIndex = value.f0;
							featureValue = value.f1;
						}
						num++;
					}
					if (num != 0) {
						out.collect(Tuple3.of(columnIndex, featureValue, num));
					}
				}
			})
			.name("count_tokens")
			.returns(new TupleTypeInfo <>(Types.INT, Types.STRING, Types.LONG));
	}

	public static DataSet <Tuple3 <Integer, Long, Long>> countLongTokens(DataSet <Tuple2 <Integer, Long>> data) {
		return data.groupBy(0, 1)
			.reduceGroup(new GroupReduceFunction <Tuple2 <Integer, Long>, Tuple3 <Integer, Long, Long>>() {
				@Override
				public void reduce(Iterable <Tuple2 <Integer, Long>> values,
								   Collector <Tuple3 <Integer, Long, Long>> out) throws Exception {
					int columnIndex = -1;
					Long featureValue = null;
					long num = 0;
					for (Tuple2 <Integer, Long> value : values) {
						if (num == 0) {
							columnIndex = value.f0;
							featureValue = value.f1;
						}
						num++;
					}
					if (num != 0) {
						out.collect(Tuple3.of(columnIndex, featureValue, num));
					}
				}
			})
			.name("count_tokens")
			.returns(new TupleTypeInfo <>(Types.INT, Types.LONG, Types.LONG));
	}

	/**
	 * Assign consecutive indices to each columns of strings, ordered alphabetically.
	 * The index space of each columns are independent.
	 *
	 * @param data        The data to index.
	 * @param startIndex  Starting index.
	 * @param isAscending If true, strings are ordered ascending by frequency.
	 * @return A DataSet of tuples of column index, token, and token index.
	 */
	public static DataSet <Tuple3 <Integer, String, Long>> indexSortedByAlphabet(
		DataSet <Tuple2 <Integer, String>> data, final long startIndex, final boolean isAscending) {

		DataSet <Tuple2 <Integer, String>> distinctTokens = distinct(data);
		DataSet <Tuple2 <Integer, Integer>> strLengthDataSet = data.mapPartition(
			new MapPartitionFunction <Tuple2 <Integer, String>, Tuple2 <Integer, Integer>>() {
				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, String>> iterable,
										 Collector <Tuple2 <Integer, Integer>> collector) throws Exception {
					HashMap <Integer, Integer> map = new HashMap <>();
					for (Tuple2 <Integer, String> value : iterable) {
						map.put(value.f0, Math.max(map.getOrDefault(value.f0, 0), value.f1.length()));
					}
					map.forEach((key, value) -> collector.collect(Tuple2.of(key, value)));
				}
			})
			.groupBy(0)
			.reduce(new ReduceFunction <Tuple2 <Integer, Integer>>() {
				@Override
				public Tuple2 <Integer, Integer> reduce(Tuple2 <Integer, Integer> v1,
														Tuple2 <Integer, Integer> v2) throws Exception {
					return Tuple2.of(v1.f0, Math.max(v1.f1, v2.f1));
				}
			})
			.name("compute_max_feature_length")
			.returns(new TupleTypeInfo <>(Types.INT, Types.INT));
		DataSet <Tuple3 <Integer, String, String>> tokenPrefix = distinctTokens.map(
			new RichMapFunction <Tuple2 <Integer, String>, Tuple3 <Integer, String, String>>() {
				HashMap <Integer, Integer> map = new HashMap <>();

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					List <Tuple2 <Integer, Integer>> list = getRuntimeContext().getBroadcastVariable("strMaxLen");
					for (Tuple2 <Integer, Integer> value : list) {
						map.put(value.f0, Math.min(value.f1 / 4, 4));
					}
				}

				@Override
				public Tuple3 <Integer, String, String> map(Tuple2 <Integer, String> value)
					throws Exception {
					int len = Math.min(value.f1.length(), map.get(value.f0));
					return Tuple3.of(value.f0, value.f1, value.f1.substring(0, len));
				}
			}).withBroadcastSet(strLengthDataSet, "strMaxLen")
			.name("generate_feature_prefix")
			.returns(new TupleTypeInfo <>(Types.INT, Types.STRING, Types.STRING));

		DataSet <Tuple3 <Integer, String, Long>> tokenPrefixCount = countStringTokens(tokenPrefix.project(0, 2));

		return tokenPrefix
			.groupBy(0, 2)
			.reduceGroup(
				new RichGroupReduceFunction <Tuple3 <Integer, String, String>, Tuple3 <Integer, String, Long>>() {
					HashMap <String, Long>[] mapList = null;

					public void open(Configuration parameters) throws Exception {
						List <Tuple3 <Integer, String, Long>> list = this.getRuntimeContext().getBroadcastVariable(
							"counts");
						HashMap <Integer, ArrayList <Tuple2 <String, Long>>> map = new HashMap <>();
						for (Tuple3 <Integer, String, Long> value : list) {
							ArrayList <Tuple2 <String, Long>> idList = map.getOrDefault(value.f0, new ArrayList <>());
							idList.add(Tuple2.of(value.f1, value.f2));
							map.put(value.f0, idList);
						}
						mapList = new HashMap[map.size()];
						for (int i = 0; i < mapList.length; i++) {
							mapList[i] = new HashMap <>();
						}
						for (Entry <Integer, ArrayList <Tuple2 <String, Long>>> entry : map.entrySet()) {
							ArrayList <Tuple2 <String, Long>> entryValue = entry.getValue();
							entryValue.sort(new Comparator <Tuple2 <String, Long>>() {
								@Override
								public int compare(Tuple2 <String, Long> o1, Tuple2 <String, Long> o2) {
									if (isAscending) {
										return String.CASE_INSENSITIVE_ORDER.compare(o1.f0, o2.f0);
									}
									return String.CASE_INSENSITIVE_ORDER.compare(o2.f0, o1.f0);
								}
							});
							long totalCount = startIndex;
							for (int i = 0; i < entryValue.size(); i++) {
								mapList[entry.getKey()].put(entryValue.get(i).f0, totalCount);
								totalCount += entryValue.get(i).f1;
							}
						}
					}

					@Override
					public void reduce(Iterable <Tuple3 <Integer, String, String>> iterable,
									   Collector <Tuple3 <Integer, String, Long>> collector) throws Exception {
						ArrayList<String> list = new ArrayList <>();
						int index = 0;
						String prefix = "";
						for (Tuple3 <Integer, String, String> value : iterable) {
							index = value.f0;
							list.add(value.f1);
							prefix = value.f2;
						}
						list.sort(new Comparator <String>() {
							@Override
							public int compare(String o1, String o2) {
								int result = String.CASE_INSENSITIVE_ORDER.compare(o1, o2);
								if (isAscending) {
									return result;
								}
								return 0 - result;
							}
						});
						Long id = mapList[index].getOrDefault(prefix, startIndex);
						for (int i = 0; i < list.size(); i++) {
							collector.collect(Tuple3.of(index, list.get(i), id + i));
						}
					}
				})
			.withBroadcastSet(tokenPrefixCount, "counts")
			.name("assign_index_alphabet_sort")
			.returns(new TupleTypeInfo <>(Types.INT, Types.STRING, Types.LONG));
	}

	public static DataSet <Tuple3 <Integer, Integer, Long>> countElementsPerPartition(
		DataSet <Tuple2 <Integer, String>> input) {
		return input.mapPartition(
			new RichMapPartitionFunction <Tuple2 <Integer, String>, Tuple3 <Integer, Integer, Long>>() {
				public void mapPartition(Iterable <Tuple2 <Integer, String>> values,
										 Collector <Tuple3 <Integer, Integer, Long>> out) throws Exception {
					HashMap <Integer, Long> counter = new HashMap <>();
					for (Tuple2 <Integer, String> value : values) {
						counter.put(value.f0, counter.getOrDefault(value.f0, 0L) + 1);
					}
					int taskId = this.getRuntimeContext().getIndexOfThisSubtask();
					for (Entry <Integer, Long> entry : counter.entrySet()) {
						out.collect(Tuple3.of(taskId, entry.getKey(), entry.getValue()));
					}
				}
			}).name("count_elements_per_partition")
			.returns(new TupleTypeInfo <>(Types.INT, Types.INT, Types.LONG));
	}

	public static DataSet <Tuple3 <Integer, String, Long>> zipWithIndex(DataSet <Tuple2 <Integer, String>> input,
																	   Long startIndex) {
		DataSet <Tuple3 <Integer, Integer, Long>> elementCount = countElementsPerPartition(input);
		return input.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, String>, Tuple3 <Integer, String, Long>>() {
			HashMap <Integer, Long> counter = new HashMap <>();

			public void open(Configuration parameters) throws Exception {
				List <Tuple3 <Integer, Integer, Long>> offsets = (List) this.getRuntimeContext().getBroadcastVariable(
					"counts");
				int taskId = this.getRuntimeContext().getIndexOfThisSubtask();
				for (Tuple3 <Integer, Integer, Long> offset : offsets) {
					if (offset.f0 < taskId) {
						counter.put(offset.f1, counter.getOrDefault(offset.f1, startIndex) + offset.f2);
					}
				}
			}

			public void mapPartition(Iterable <Tuple2 <Integer, String>> values, Collector <Tuple3 <Integer, String, Long>> out)
				throws Exception {
				for (Tuple2 <Integer, String> value : values) {
					long id = counter.getOrDefault(value.f0, startIndex);
					out.collect(Tuple3.of(value.f0, value.f1, id));
					counter.put(value.f0, id + 1);
				}
			}
		}).withBroadcastSet(elementCount, "counts")
			.name("assign_index")
			.returns(new TupleTypeInfo <>(Types.INT, Types.STRING, Types.LONG));
	}
}
