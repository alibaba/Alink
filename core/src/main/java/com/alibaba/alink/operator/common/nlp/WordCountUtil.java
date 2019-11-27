package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.operator.common.dataproc.SortUtils.RowComparator;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.RowUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WordCountUtil {
	public static final String WORD_COL_NAME = "word";
	public static final String COUNT_COL_NAME = "cnt";
	public static final String INDEX_COL_NAME = "idx";
	public final static int BOUND_SIZE = 10000;
	private final static Logger LOG = LoggerFactory.getLogger(WordCountUtil.class);

	public static BatchOperator<?> splitDocAndCount(BatchOperator<?> input, String docColName, String wordDelimiter) {
		return count(splitDoc(input, docColName, wordDelimiter), WORD_COL_NAME, COUNT_COL_NAME);
	}

	public static BatchOperator splitDoc(BatchOperator<?> input, String docColName, String wordDelimiter) {
		return input.udtf(
			docColName,
			new String[] {WORD_COL_NAME, COUNT_COL_NAME},
			new DocWordSplitCount(wordDelimiter),
			new String[] {}
		);
	}

	public static BatchOperator count(BatchOperator input, String wordColName) {
		return count(input, wordColName, null);
	}

	public static BatchOperator count(BatchOperator input, String wordColName, String wordValueColName) {
		if (null == wordValueColName) {
			return input.groupBy(wordColName,
				wordColName + " AS " + WORD_COL_NAME + ", COUNT(" + wordColName + ") AS " + COUNT_COL_NAME);
		} else {
			return input.groupBy(wordColName,
				wordColName + " AS " + WORD_COL_NAME + ", SUM(" + wordValueColName + ") AS " + COUNT_COL_NAME);
		}
	}

	public static BatchOperator randomIndexVocab(BatchOperator vocab, long startIndex) {
		TableSchema schema = vocab.getSchema();
		return new TableSourceBatchOp(
			DataSetConversionUtil.toTable(vocab.getMLEnvironmentId(),
				randomIndexVocab(vocab.getDataSet(), startIndex),
				ArrayUtils.add(schema.getFieldNames(), INDEX_COL_NAME),
				ArrayUtils.add(schema.getFieldTypes(), Types.LONG)
			)
		).setMLEnvironmentId(vocab.getMLEnvironmentId());
	}

	public static DataSet <Row> randomIndexVocab(DataSet <Row> vocab, long startIndex) {
		return DataSetUtils.zipWithIndex(vocab)
			.map(new RandomIndexMapper(startIndex));
	}

	public static Tuple3 <DataSet <Row>, DataSet <Long[]>, DataSet <long[]>> sortedIndexVocab(
		DataSet <Row> weightedVocab,
		final long gStartIdx) {
		return sortedIndexVocab(weightedVocab, gStartIdx, false);
	}

	/**
	 * @param weightedVocab includes 3 columns, {word, cnt, weight}, with type {string, long, double}
	 * @return
	 */
	public static Tuple3 <DataSet <Row>, DataSet <Long[]>, DataSet <long[]>> sortedIndexVocab(
		DataSet <Row> weightedVocab,
		final long gStartIdx,
		final boolean hasGroupType) {
		DataSet <Tuple2 <Object, Integer>> groupType = null;

		if (hasGroupType) {
			groupType = weightedVocab
				.groupBy(3)
				.reduceGroup(new GroupReduceFunction <Row, Object>() {
					@Override
					public void reduce(Iterable <Row> values, Collector <Object> out) throws Exception {
						Object curKey = null;
						for (Row val : values) {
							curKey = val.getField(3);
						}

						out.collect(curKey);
					}
				})
				.reduceGroup(new GroupReduceFunction <Object, Tuple2 <Object, Integer>>() {
					@Override
					public void reduce(Iterable <Object> values, Collector <Tuple2 <Object, Integer>> out)
						throws Exception {
						int idx = 0;

						for (Object val : values) {
							out.collect(Tuple2.of(val, idx++));
						}
					}
				});
		}

		Tuple2 <DataSet <Tuple2 <Integer, Row>>, DataSet <Tuple2 <Integer, Long>>> sorted
			= SortUtils.pSort(weightedVocab, 2);

		DataSet <Tuple2 <Integer, Row>> partitioned = sorted
			.f0
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key;
				}
			}, 0);

		MapPartitionOperator <Tuple2 <Integer, Row>, Tuple2 <Integer, long[]>> instSum = partitioned
			.mapPartition(
				new RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, long[]>>() {
					int instId;
					Map <Object, Integer> typeMap = new HashMap <>();

					@Override
					public void open(Configuration parameters) throws Exception {
						instId = getRuntimeContext().getIndexOfThisSubtask();

						if (hasGroupType) {
							List <Tuple2 <Object, Integer>> types = getRuntimeContext()
								.getBroadcastVariable("w2vGroupTypes");

							for (Tuple2 <Object, Integer> type : types) {
								typeMap.put(type.f0, type.f1);
							}
						}
					}

					@Override
					public void mapPartition(Iterable <Tuple2 <Integer, Row>> values,
											 Collector <Tuple2 <Integer, long[]>> out)
						throws Exception {
						long[] ret;

						if (hasGroupType) {
							ret = new long[typeMap.keySet().size()];
						} else {
							ret = new long[1];
						}

						for (Tuple2 <Integer, Row> val : values) {
							if (hasGroupType) {
								ret[typeMap.get(val.f1.getField(3))] += 1L;
							} else {
								ret[0] += 1L;
							}
						}

						out.collect(Tuple2.of(instId, ret));
					}
				}
			);

		if (hasGroupType) {
			instSum = instSum.withBroadcastSet(groupType, "w2vGroupTypes");
		}

		MapPartitionOperator <Tuple2 <Integer, Row>, Tuple2 <Integer, double[]>> weightInstSum = partitioned
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2 <Integer, double[]>>() {
				int instId;
				Map <Object, Integer> typeMap = new HashMap <>();

				@Override
				public void open(Configuration parameters) throws Exception {
					instId = getRuntimeContext().getIndexOfThisSubtask();

					if (hasGroupType) {
						List <Tuple2 <Object, Integer>> types = getRuntimeContext()
							.getBroadcastVariable("w2vGroupTypes");

						for (Tuple2 <Object, Integer> type : types) {
							typeMap.put(type.f0, type.f1);
						}
					}
				}

				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, Row>> values,
										 Collector <Tuple2 <Integer, double[]>> out)
					throws Exception {

					double[] ret;

					if (hasGroupType) {
						ret = new double[typeMap.keySet().size()];
					} else {
						ret = new double[1];
					}

					for (Tuple2 <Integer, Row> val : values) {
						if (hasGroupType) {
							ret[typeMap.get(val.f1.getField(3))] += (double) val.f1.getField(2);
						} else {
							ret[0] += (double) val.f1.getField(2);
						}
					}

					out.collect(new Tuple2 <>(instId, ret));
				}
			});

		if (hasGroupType) {
			weightInstSum = weightInstSum.withBroadcastSet(groupType, "w2vGroupTypes");
		}

		/**
		 * vocab, count, weight, id
		 */
		MapPartitionOperator <Tuple2 <Integer, Row>, Row> vocabWithIdAndBound = partitioned
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, Row>, Row>() {
				int size;
				long[] startIdx;
				long[] totalCountIdx;
				//                long[] cumCountIdx;

				double[] weightStart;
				double[] weightTotal;
				double[] curWeightTotal;
				boolean isFirstPartition;

				Map <Object, Integer> typeMap = new HashMap <>();

				@Override
				public void open(Configuration parameters) throws Exception {
					size = 1;

					if (hasGroupType) {
						List <Tuple2 <Object, Integer>> types = getRuntimeContext().getBroadcastVariable(
							"w2vGroupTypes");

						for (Tuple2 <Object, Integer> type : types) {
							typeMap.put(type.f0, type.f1);
						}

						size = typeMap.size();
					}

					/**
					 * for id
					 */
					List <Tuple2 <Integer, long[]>> instVocabSizeList
						= getRuntimeContext().getBroadcastVariable("w2vInstVocabSize");

					LOG.info("w2vInstVocabSize: {}", JsonConverter.gson.toJson(instVocabSizeList));

					int instId = getRuntimeContext().getIndexOfThisSubtask();

					isFirstPartition = (0 == instId);

					startIdx = new long[size];
					totalCountIdx = new long[size];
					long[] cumCountIdx = new long[size];
					for (Tuple2 <Integer, long[]> instVocabSize : instVocabSizeList) {
						for (int i = 0; i < size; ++i) {
							if (instVocabSize.f0 < instId) {
								startIdx[i] += instVocabSize.f1[i];
							}

							totalCountIdx[i] += instVocabSize.f1[i];

							if (i == 0) {
								cumCountIdx[i] = totalCountIdx[i];
							} else {
								cumCountIdx[i] = cumCountIdx[i - 1] + totalCountIdx[i];
							}
						}
					}

					for (int i = 1; i < size; ++i) {
						startIdx[i] += cumCountIdx[i - 1];
					}

					/**
					 * for weight interval
					 */
					List <Tuple2 <Integer, double[]>> weightInstSumList =
						getRuntimeContext().getBroadcastVariable("w2vWeightInstSum");

					weightStart = new double[size];
					weightTotal = new double[size];
					curWeightTotal = new double[size];

					for (Tuple2 <Integer, double[]> weightInstSum : weightInstSumList) {
						for (int i = 0; i < size; ++i) {
							if (weightInstSum.f0 < instId) {
								weightStart[i] += weightInstSum.f1[i];
							}

							weightTotal[i] += weightInstSum.f1[i];

							if (weightInstSum.f0 == instId) {
								curWeightTotal[i] = weightInstSum.f1[i];
							}
						}
					}
				}

				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, Row>> values, Collector <Row> out)
					throws Exception {
					ArrayList <Row> valuesList = new ArrayList <>();

					for (Tuple2 <Integer, Row> value : values) {
						valuesList.add(value.f1);
					}

					Collections.sort(valuesList, new RowComparator(2));

					//System.out.println("taskid: " + getRuntimeContext().getIndexOfThisSubtask()
					//    + ", val: " + valuesList.toString());
					/**
					 * start index from 1 for model.
					 * use positive for model input.
					 * use negative for model output.
					 */
					long[] localStart = new long[size];
					for (int i = 0; i < size; ++i) {
						localStart[i] = gStartIdx;
					}

					double[] localCumWeight = new double[size];

					if (isFirstPartition) {
						for (int i = 0; i < size; ++i) {
							out.collect(
								Row.of(null, startIdx[i] + localStart[i], null, -(long) ((BOUND_SIZE + 1) * i + 1)));
							out.collect(Row.of(null, totalCountIdx[i] - 1 + localStart[i], null,
								-(long) ((BOUND_SIZE + 1) * (i + 1))));
						}
					}

					long[] boundIndex = new long[size];

					for (int i = 0; i < size; ++i) {
						boundIndex[i] = (long) Math.floor(weightStart[i] / weightTotal[i] * BOUND_SIZE);
						if (weightStart[i] / weightTotal[i] * BOUND_SIZE <= boundIndex[i] && curWeightTotal[i] > 0.) {
							out.collect(Row.of(null, startIdx[i] + localStart[i], null,
								-(boundIndex[i] + (BOUND_SIZE + 1) * i + 1)));
						}
					}

					int type = 0;

					for (Row value : valuesList) {
						if (hasGroupType) {
							type = typeMap.get(value.getField(3));
						}

						localCumWeight[type] += (double) value.getField(2);

						double globalCumWeight = localCumWeight[type] + weightStart[type];
						while (globalCumWeight / weightTotal[type] * BOUND_SIZE >= boundIndex[type] + 1) {
							boundIndex[type]++;
							out.collect(Row.of(null, startIdx[type] + localStart[type], null,
								-(boundIndex[type] + (BOUND_SIZE + 1) * type + 1)));
						}

						out.collect(Row.of(
							value.getField(0),
							value.getField(1),
							value.getField(2),
							startIdx[type] + localStart[type]));

						localStart[type]++;
					}
				}
			})
			.withBroadcastSet(instSum, "w2vInstVocabSize")
			.withBroadcastSet(weightInstSum, "w2vWeightInstSum");

		if (hasGroupType) {
			vocabWithIdAndBound = vocabWithIdAndBound.withBroadcastSet(groupType, "w2vGroupTypes");
		}

		DataSet <Row> vocabWithId = vocabWithIdAndBound.filter(new FilterFunction <Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return (long) value.getField(3) > 0;
			}
		}).map(new MapFunction <Row, Row>() {
			@Override
			public Row map(Row value) throws Exception {
				return Row.of(value.getField(0), value.getField(1), value.getField(3));
			}
		});

		GroupReduceOperator <Tuple2 <Long, Long>, Long[]> bound = vocabWithIdAndBound
			.filter(new FilterFunction <Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					//System.out.println("filter: " + value.toString());
					return (long) value.getField(3) < 0;
				}
			})
			.map(new MapFunction <Row, Tuple2 <Long, Long>>() {
				@Override
				public Tuple2 <Long, Long> map(Row value) throws Exception {
					return new Tuple2 <Long, Long>(-(Long) value.getField(3) - 1L, (Long) value.getField(1));
				}
			})
			.reduceGroup(new RichGroupReduceFunction <Tuple2 <Long, Long>, Long[]>() {
				int size;

				@Override
				public void open(Configuration parameters) throws Exception {
					size = 1;
					if (hasGroupType) {
						List <Tuple2 <Object, Integer>> types = getRuntimeContext().getBroadcastVariable(
							"w2vGroupTypes");

						size = types.size();
					}
				}

				@Override
				public void reduce(Iterable <Tuple2 <Long, Long>> values, Collector <Long[]> out) throws Exception {
					HashMap <Long, Long> map = new HashMap <>();
					for (Tuple2 <Long, Long> value : values) {
						map.put(value.f0, value.f1);
					}

					Long[] bound = new Long[(BOUND_SIZE + 1) * size];
					for (int i = 0; i < (BOUND_SIZE + 1) * size; i++) {
						if (map.containsKey((long) i)) {
							bound[i] = map.get((long) i);
						} else {
							bound[i] = bound[i - 1];
						}
					}

					out.collect(bound);
				}
			});

		if (hasGroupType) {
			bound = bound.withBroadcastSet(groupType, "w2vGroupTypes");
		}

		DataSet <long[]> typeStart = null;

		if (hasGroupType) {
			typeStart = groupType
				.reduceGroup(new RichGroupReduceFunction <Tuple2 <Object, Integer>, long[]>() {
					long[] startIdx;
					int size;

					@Override
					public void open(Configuration parameters) throws Exception {
						List <Tuple2 <Object, Integer>> types = getRuntimeContext().getBroadcastVariable(
							"w2vGroupTypes");

						size = types.size();

						List <Tuple2 <Integer, long[]>> instVocabSizeList
							= getRuntimeContext().getBroadcastVariable("w2vInstVocabSize");

						int instId = getRuntimeContext().getIndexOfThisSubtask();

						startIdx = new long[size];
						long[] countIdx = new long[size];
						long[] cumCountIdx = new long[size];
						for (Tuple2 <Integer, long[]> instVocabSize : instVocabSizeList) {
							for (int i = 0; i < size; ++i) {
								if (instVocabSize.f0 < instId) {
									startIdx[i] += instVocabSize.f1[i];
								}

								countIdx[i] += instVocabSize.f1[i];
								if (i == 0) {
									cumCountIdx[i] = countIdx[i];
								} else {
									cumCountIdx[i] = cumCountIdx[i - 1] + countIdx[i];
								}
							}
						}

						for (int i = 1; i < size; ++i) {
							startIdx[i] += cumCountIdx[i - 1];
						}
					}

					@Override
					public void reduce(Iterable <Tuple2 <Object, Integer>> values,
									   Collector <long[]> out) throws Exception {

						Object[] objs = new Object[size];
						long[] startIdxes = new long[size];
						for (Tuple2 <Object, Integer> val : values) {
							objs[val.f1] = val.f0;
							startIdxes[val.f1] = startIdx[val.f1] + gStartIdx;
						}

						out.collect(startIdxes);
					}
				})
				.withBroadcastSet(instSum, "w2vInstVocabSize")
				.withBroadcastSet(groupType, "w2vGroupTypes");
		}

		return new Tuple3 <>(vocabWithId, bound, typeStart);
	}

	public static BatchOperator transWord2Index(BatchOperator in, String[] selectedColNames, String[] keepColNames,
												BatchOperator indexedVocab) {
		return transWord2Index(in, selectedColNames, keepColNames, indexedVocab, WORD_COL_NAME, INDEX_COL_NAME);
	}

	public static BatchOperator transWord2Index(BatchOperator in, String[] selectedColNames, String[] keepColNames,
												BatchOperator indexedVocab, String wordColName, String idxColName) {
		return trans(in, selectedColNames, keepColNames, indexedVocab, wordColName, idxColName, true, null);
	}

	public static BatchOperator transDoc2IndexVector(BatchOperator in, String[] selectedColNames, String wordDelimiter,
													 String[] keepColNames, BatchOperator indexedVocab) {
		return transDoc2IndexVector(in, selectedColNames, wordDelimiter, keepColNames, indexedVocab, WORD_COL_NAME,
			INDEX_COL_NAME);
	}

	public static BatchOperator transDoc2IndexVector(BatchOperator in, String[] selectedColNames, String wordDelimiter,
													 String[] keepColNames,
													 BatchOperator indexedVocab, String wordColName,
													 String idxColName) {
		return trans(in, selectedColNames, keepColNames, indexedVocab, wordColName, idxColName, false, wordDelimiter);
	}

	private static BatchOperator trans(BatchOperator in, String[] selectedColNames, String[] keepColNames,
									   BatchOperator indexedVocab, String wordColName, String idxColName,
									   boolean isWord, String wordDelimiter) {
		String[] colnames = in.getColNames();
		TypeInformation <?>[] coltypes = in.getColTypes();
		int[] colIdxs = findColIdx(selectedColNames, colnames, coltypes);

		int[] appendIdxs = findAppendColIdx(keepColNames, colnames);

		// only 2 cols: word, idx
		DataSet <Row> voc = indexedVocab.select(wordColName + "," + idxColName).getDataSet();

		DataSet <Row> contentMapping = in.getDataSet()
			.map(new GenContentMapping(colIdxs, appendIdxs, isWord, wordDelimiter))
			.withBroadcastSet(voc, "vocabulary");

		int transColSize = colIdxs.length;
		int keepColSize = keepColNames == null ? 0 : keepColNames.length;
		int outputColSize = transColSize + keepColSize;
		String[] names = new String[outputColSize];
		TypeInformation <?>[] types = new TypeInformation <?>[outputColSize];
		int i = 0;
		for (; i < transColSize; ++i) {
			names[i] = colnames[colIdxs[i]];
			types[i] = isWord ? Types.DOUBLE : Types.STRING;
		}
		for (; i < outputColSize; ++i) {
			names[i] = colnames[appendIdxs[i - transColSize]];
			types[i] = coltypes[appendIdxs[i - transColSize]];
		}

		return new TableSourceBatchOp(DataSetConversionUtil.toTable(in.getMLEnvironmentId(), contentMapping, names, types))
			.setMLEnvironmentId(in.getMLEnvironmentId());
	}

	private static int[] findColIdx(String[] selectedColNames, String[] colnames, TypeInformation <?>[] coltypes) {
		int size = selectedColNames.length;
		if (size < 1 || size > colnames.length) {
			throw new RuntimeException(String.format(
				"selected column size out of range [%d, %d]", 1, colnames.length));
		}
		int[] colIdxs = new int[size];

		for (int i = 0; i < size; ++i) {
			colIdxs[i] = -1;
			for (int j = 0; j < colnames.length; ++j) {
				if (selectedColNames[i].equals(colnames[j])) {
					if (coltypes[j] != Types.STRING) {
						throw new RuntimeException(String.format(
							"type of column: %s must be string.", colnames[j]
						));
					}
					colIdxs[i] = j;
					break;
				}
			}
			if (colIdxs[i] == -1) {
				throw new RuntimeException(String.format(
					"column %s does not exist.", selectedColNames[i]));
			}
		}
		return colIdxs;
	}

	private static int[] findAppendColIdx(String[] keepColNames, String[] colnames) {
		if (keepColNames == null || keepColNames.length < 1) {
			return null;
		}

		int size = keepColNames.length;
		if (size > colnames.length) {
			throw new RuntimeException(String.format(
				"selected append column size out of range [%d, %d]", 0, colnames.length));
		}
		int[] colIdxs = new int[size];

		for (int i = 0; i < size; ++i) {
			colIdxs[i] = -1;
			for (int j = 0; j < colnames.length; ++j) {
				if (keepColNames[i].equals(colnames[j])) {
					colIdxs[i] = j;
					break;
				}
			}
			if (colIdxs[i] == -1) {
				throw new RuntimeException(String.format(
					"column %s does not exist.", keepColNames[i]));
			}
		}
		return colIdxs;
	}

	/**
	 * Sort a partitioned dataset.
	 *
	 * @param partitioned
	 * @param partitionCnt
	 * @return
	 */
	public static DataSet <Tuple2 <Long, Row>> localSort(DataSet <Tuple2 <Integer, Row>> partitioned,
														 DataSet <Tuple2 <Integer, Long>> partitionCnt,
														 final int field) {
		return partitioned
			.partitionCustom(new Partitioner <Integer>() {
				@Override
				public int partition(Integer key, int numPartitions) {
					return key % numPartitions;
				}
			}, 0)
			.mapPartition(new RichMapPartitionFunction <Tuple2 <Integer, Row>, Tuple2 <Long, Row>>() {
				transient long startIdx;

				@Override
				public void open(Configuration parameters) throws Exception {
					List <Tuple2 <Integer, Long>> bc = getRuntimeContext().getBroadcastVariable("partitionCnt");
					startIdx = 0L;
					int taskId = getRuntimeContext().getIndexOfThisSubtask();
					for (Tuple2 <Integer, Long> pcnt : bc) {
						if (pcnt.f0 < taskId) {
							startIdx += pcnt.f1;
						}
					}
				}

				@Override
				public void mapPartition(Iterable <Tuple2 <Integer, Row>> values, Collector <Tuple2 <Long, Row>> out)
					throws Exception {
					ArrayList <Row> valuesList = new ArrayList <>();

					for (Tuple2 <Integer, Row> value : values) {
						valuesList.add(value.f1);
					}

					Collections.sort(valuesList, new RowComparator(field));
					long cnt = 0L;

					for (Row row : valuesList) {
						out.collect(Tuple2.of(startIdx + cnt, row));
						cnt++;
					}
				}
			})
			.name("local_sort")
			.withBroadcastSet(partitionCnt, "partitionCnt");
	}

	private static class RandomIndexMapper implements MapFunction <Tuple2 <Long, Row>, Row> {

		private final long startIndex;

		public RandomIndexMapper(long startIndex) {
			this.startIndex = startIndex;
		}

		@Override
		public Row map(Tuple2 <Long, Row> value) throws Exception {
			Row ret = RowUtil.merge(value.f1, (Long) value.f0 + this.startIndex);
			return ret;
		}
	}

	public static final class GenContentMapping extends RichMapFunction <Row, Row> {

		private final boolean isWord;
		private final String wordDelimiter;
		private final int transColSize;
		private final int keepColSize;
		private final int outputColSize;
		private int[] colIdxs;
		private int[] appendColIdxs;
		private Map <String, Long> vocMap;

		public GenContentMapping(int[] colIdxs, int[] appendColIdxs, boolean isWord, String wordDelimiter) {
			this.colIdxs = colIdxs;
			this.appendColIdxs = appendColIdxs;
			this.isWord = isWord;
			this.wordDelimiter = wordDelimiter;

			this.transColSize = this.colIdxs.length;
			this.keepColSize = this.appendColIdxs == null ? 0 : this.appendColIdxs.length;
			this.outputColSize = transColSize + keepColSize;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Row> voc = getRuntimeContext().getBroadcastVariable("vocabulary");
			vocMap = new HashMap <>();
			for (Row row : voc) {
				vocMap.put((String) row.getField(0), (Long) row.getField(1));
			}
		}

		@Override
		public Row map(Row row) throws Exception {
			Row row2 = new Row(outputColSize);

			for (int i = 0; i < transColSize; ++i) {
				if (isWord) {
					String word = (String) row.getField(colIdxs[i]);
					row2.setField(i, vocMap.containsKey(word) ? vocMap.get(word) : null);
				} else {
					String content = (String) row.getField(colIdxs[i]);
					String[] words = content.split(wordDelimiter);
					StringBuilder builder = new StringBuilder();
					int cnt = 0;
					for (String word : words) {
						if (!vocMap.containsKey(word)) {
							continue;
						}
						if (cnt > 0) {
							builder.append(",");
						}
						builder.append(vocMap.get(word).toString());
						++cnt;
					}
					row2.setField(i, builder.toString());
				}
			}

			for (int i = 0; i < keepColSize; ++i) {
				row2.setField(transColSize + i, row.getField(appendColIdxs[i]));
			}

			return row2;
		}
	}

	public static class WordSpliter implements FlatMapFunction <Row, String[]> {
		private String wordDelimiter;

		public WordSpliter(String wordDelimiter) {
			this.wordDelimiter = wordDelimiter;
		}

		@Override
		public void flatMap(Row row, Collector <String[]> collector) throws Exception {
			if (null != row.getField(0)) {
				collector.collect(((String) row.getField(0)).split(this.wordDelimiter));
			}
		}
	}
}
