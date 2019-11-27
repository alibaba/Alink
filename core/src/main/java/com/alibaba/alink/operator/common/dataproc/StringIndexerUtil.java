package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class to assign indices to columns of strings.
 */
public class StringIndexerUtil {

    /**
     * Specifies how to order tokens.
     */
    public enum OrderType {
        /**
         * Index randomly.
         */
        RANDOM,
        /**
         * Ordered by token frequency in ascending order.
         */
        FREQUENCY_ASC,
        /**
         * Ordered by token frequency in descending order.
         */
        FREQUENCY_DESC,
        /**
         * Ordered by ascending alphabet order ascending.
         */
        ALPHABET_ASC,
        /**
         * Ordered by descending alphabet order ascending.
         */
        ALPHABET_DESC
    }

    /**
     * Strategy to handle unseen token when doing prediction.
     */
    public enum HandleInvalidStrategy {
        /**
         * Assign "max index" + 1.
         */
        KEEP,
        /**
         * Raise exception.
         */
        ERROR,
        /**
         * Pad with null.
         */
        SKIP
    }

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
    public static DataSet<Tuple3<Integer, String, Long>> indexTokens(
        DataSet<Row> data, OrderType orderType, final long startIndex, final boolean ignoreNull) {

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
                throw new IllegalArgumentException("Unsupported order type " + orderType);
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
    private static DataSet<Tuple3<Integer, String, Long>> indexRandom(
        DataSet<Row> data, final long startIndex, final boolean ignoreNull) {

        DataSet<Tuple2<Integer, String>> distinctTokens = flattenTokens(data, ignoreNull)
            .distinct()
            .name("distinct_tokens");

        return zipWithIndexPerColumn(distinctTokens)
            .map(new MapFunction<Tuple3<Long, Integer, String>, Tuple3<Integer, String, Long>>() {
                @Override
                public Tuple3<Integer, String, Long> map(Tuple3<Long, Integer, String> value) throws Exception {
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
    private static DataSet<Tuple3<Integer, String, Long>> indexSortedByFreq(
        DataSet<Row> data, final long startIndex, final boolean ignoreNull, final boolean isAscending) {

        DataSet<Tuple3<Integer, String, Long>> distinctTokens = flattenTokens(data, ignoreNull)
            .map(new MapFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Long>>() {
                @Override
                public Tuple3<Integer, String, Long> map(Tuple2<Integer, String> value) throws Exception {
                    return Tuple3.of(value.f0, value.f1, 1L);
                }
            })
            .groupBy(0, 1)
            .reduce(new ReduceFunction<Tuple3<Integer, String, Long>>() {
                @Override
                public Tuple3<Integer, String, Long> reduce(Tuple3<Integer, String, Long> value1,
                                                            Tuple3<Integer, String, Long> value2) throws Exception {
                    value1.f2 += value2.f2;
                    return value1;
                }
            })
            .name("count_tokens");

        return distinctTokens
            .groupBy(0)
            .sortGroup(2, isAscending ? Order.ASCENDING : Order.DESCENDING)
            .reduceGroup(new GroupReduceFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, String, Long>>() {
                @Override
                public void reduce(Iterable<Tuple3<Integer, String, Long>> values,
                                   Collector<Tuple3<Integer, String, Long>> out) throws Exception {
                    long id = startIndex;
                    for (Tuple3<Integer, String, Long> value : values) {
                        out.collect(Tuple3.of(value.f0, value.f1, id++));
                    }
                }
            });
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
    private static DataSet<Tuple3<Integer, String, Long>> indexSortedByAlphabet(
        DataSet<Row> data, final long startIndex, final boolean ignoreNull, final boolean isAscending) {

        DataSet<Tuple2<Integer, String>> distinctTokens = flattenTokens(data, ignoreNull)
            .distinct()
            .name("distinct_tokens");

        return distinctTokens
            .groupBy(0)
            .reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Long>>() {
                @Override
                public void reduce(Iterable<Tuple2<Integer, String>> values,
                                   Collector<Tuple3<Integer, String, Long>> out) throws Exception {
                    int col = -1;
                    List<String> tokenList = new ArrayList<>();
                    for (Tuple2<Integer, String> v : values) {
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
    private static DataSet<Tuple2<Integer, String>> flattenTokens(DataSet<Row> data, final boolean ignoreNull) {
        return data
            .flatMap(new FlatMapFunction<Row, Tuple2<Integer, String>>() {
                @Override
                public void flatMap(Row value, Collector<Tuple2<Integer, String>> out) throws Exception {
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
     * Count tokens per partition per column.
     *
     * @param input The flattened token, a DataSet of column index and token.
     * @return A DataSet of tuples of subtask index, column index, number of tokens.
     */
    private static DataSet<Tuple3<Integer, Integer, Long>> countTokensPerPartitionPerColumn(
        DataSet<Tuple2<Integer, String>> input) {
        return input.mapPartition(
            new RichMapPartitionFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, Long>>() {
                @Override
                public void mapPartition(Iterable<Tuple2<Integer, String>> values,
                                         Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
                    Map<Integer, Long> counter = new HashMap<>(); // column -> count
                    for (Tuple2<Integer, String> value : values) {
                        counter.merge(value.f0, 1L, Long::sum);
                    }
                    int taskId = getRuntimeContext().getIndexOfThisSubtask();
                    counter.forEach((k, v) -> out.collect(Tuple3.of(taskId, k, v)));
                }
            });
    }

    /**
     * Assign ids to all tokens. Each columns of tokens are indexed independently.
     *
     * @param input The input data set consisting of group index and value.
     * @return A data set of tuple 3 consisting of token id, group index and token.
     */
    private static DataSet<Tuple3<Long, Integer, String>> zipWithIndexPerColumn(DataSet<Tuple2<Integer, String>> input) {

        DataSet<Tuple3<Integer, Integer, Long>> tokenCountsPerPartitionPerColumn = countTokensPerPartitionPerColumn(input);

        return input
            .map(new RichMapFunction<Tuple2<Integer, String>, Tuple3<Long, Integer, String>>() {
                /**
                 * A map from column index to its next index to assign.
                 */
                transient Map<Integer, Long> columnNextIndex;

                @Override
                public void open(Configuration parameters) throws Exception {
                    List<Tuple3<Integer, Integer, Long>> bc = getRuntimeContext()
                        .getBroadcastVariable("tokenCountsPerPartitionPerColumn");

                    int taskId = getRuntimeContext().getIndexOfThisSubtask();
                    columnNextIndex = new HashMap<>();
                    for (Tuple3<Integer, Integer, Long> count : bc) {
                        int partitionId = count.f0;
                        int columnIndex = count.f1;
                        long cnt = count.f2;
                        if (!columnNextIndex.containsKey(columnIndex)) {
                            columnNextIndex.put(columnIndex, 0L);
                        }
                        if (partitionId < taskId) {
                            columnNextIndex.merge(columnIndex, cnt, Long::sum);
                        }
                    }
                }

                @Override
                public Tuple3<Long, Integer, String> map(Tuple2<Integer, String> value) throws Exception {
                    Long index = columnNextIndex.get(value.f0);
                    columnNextIndex.replace(value.f0, index + 1L);
                    return new Tuple3<>(index, value.f0, value.f1);
                }
            })
            .withBroadcastSet(tokenCountsPerPartitionPerColumn, "tokenCountsPerPartitionPerColumn")
            .withForwardedFields("f0->f1;f1->f2");
    }
}
