package com.alibaba.alink.operator.batch.associationrule;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.associationrule.ParallelPrefixSpan;
import com.alibaba.alink.operator.common.associationrule.SequenceRule;
import com.alibaba.alink.params.associationrule.PrefixSpanParams;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * PrefixSpan algorithm is used to mine frequent sequential patterns.
 * The PrefixSpan algorithm is described in J. Pei, et al.,
 * Mining Sequential Patterns by Pattern-Growth: The PrefixSpan Approach
 */
public final class PrefixSpanBatchOp extends BatchOperator<PrefixSpanBatchOp>
    implements PrefixSpanParams<PrefixSpanBatchOp> {
    private static final Logger LOG = LoggerFactory.getLogger(PrefixSpanBatchOp.class);

    /**
     * The separator between items in an element.
     */
    public static final String ITEM_SEPARATOR = ",";

    /**
     * The separator between elements in a sequence.
     */
    public static final String ELEMENT_SEPARATOR = ";";

    /**
     * The separator between antecedent and consequent in the rules.
     */
    public static final String RULE_SEPARATOR = "=>";

    private static final String[] ITEMSETS_COL_NAMES = new String[]{"itemset", "supportcount", "itemcount"};

    private static final String[] RULES_COL_NAMES = new String[]{"rule", "chain_length", "support",
        "confidence", "transaction_count"};

    private static final TypeInformation[] ITEMSETS_COL_TYPES = new TypeInformation[]{
        Types.STRING, Types.LONG, Types.LONG};

    private static final TypeInformation[] RULES_COL_TYPES = new TypeInformation[]{
        Types.STRING, Types.LONG, Types.DOUBLE, Types.DOUBLE, Types.LONG};

    public PrefixSpanBatchOp() {
        this(new Params());
    }

    public PrefixSpanBatchOp(Params params) {
        super(params);
    }

    @Override
    public PrefixSpanBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        final String itemsColName = getItemsCol();
        final double minSupportPercent = getMinSupportPercent();
        final int minSupportCount = getMinSupportCount();
        final int maxPatternLength = getMaxPatternLength();
        final double minConfidence = getMinConfidence();
        final int itemsColIdx = TableUtil.findColIndex(in.getSchema(), itemsColName);

        Preconditions.checkArgument(itemsColIdx >= 0, "Can't find column: " + itemsColName);

        DataSet<Long> sequenceCount = count(in.getDataSet());
        DataSet<Long> minSupportCnt = getMinSupportCnt(sequenceCount, minSupportCount, minSupportPercent);

        DataSet<List<List<String>>> inputSequences = ((DataSet<Row>) in.getDataSet())
            .map(new MapFunction<Row, List<List<String>>>() {
                @Override
                public List<List<String>> map(Row row) throws Exception {
                    String sequence = (String) row.getField(itemsColIdx);
                    if (StringUtils.isNullOrWhitespaceOnly(sequence)) {
                        return new ArrayList<>();
                    }
                    String[] elements = sequence.split(ELEMENT_SEPARATOR);
                    List<List<String>> ret = new ArrayList<>(elements.length);
                    for (String element : elements) {
                        String[] items = element.trim().split(ITEM_SEPARATOR);
                        ret.add(Arrays.asList(items));
                    }
                    return ret;
                }
            })
            .name("split_sequences");

        // Count the support of each items.
        DataSet<Tuple2<String, Integer>> itemCounts = inputSequences
            .flatMap(new FlatMapFunction<List<List<String>>, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(List<List<String>> sequence, Collector<Tuple2<String, Integer>> out) throws Exception {
                    sequence.forEach(
                        s -> {
                            s.forEach(t -> {
                                out.collect(Tuple2.of(t, 1));
                            });
                        }
                    );
                }
            })
            .groupBy(0)
            .aggregate(Aggregations.SUM, 1);

        // Drop items with support smaller than requirement.
        DataSet<Tuple2<String, Integer>> qualifiedItems = itemCounts
            .filter(new RichFilterFunction<Tuple2<String, Integer>>() {
                transient Long minSupportCount;

                @Override
                public void open(Configuration parameters) throws Exception {
                    List<Long> bc = getRuntimeContext().getBroadcastVariable("minSupportCnt");
                    minSupportCount = bc.get(0);
                    LOG.info("minSupportCnt {}", minSupportCount);
                }

                @Override
                public boolean filter(Tuple2<String, Integer> value) throws Exception {
                    return value.f1 >= minSupportCount;
                }
            })
            .withBroadcastSet(minSupportCnt, "minSupportCnt")
            .name("get_qualified_items");

        // Assign items with indices, ordered by their support.
        DataSet<Tuple2<String, Integer>> itemIndex = in.getDataSet().getExecutionEnvironment().fromElements(0)
            .flatMap(new RichFlatMapFunction<Integer, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(Integer value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    List<Tuple2<String, Integer>> bc = getRuntimeContext().getBroadcastVariable("qualifiedItems");
                    Integer[] order = new Integer[bc.size()];
                    for (int i = 0; i < order.length; i++) {
                        order[i] = i;
                    }
                    Arrays.sort(order, (o1, o2) -> {
                        Integer cnt1 = bc.get(o1).f1;
                        Integer cnt2 = bc.get(o2).f1;
                        if (cnt1.equals(cnt2)) {
                            return bc.get(o1).f0.compareTo(bc.get(o2).f0);
                        }
                        return Integer.compare(cnt2, cnt1);
                    });
                    for (int i = 0; i < order.length; i++) {
                        out.collect(Tuple2.of(bc.get(order[i]).f0, i + 1)); // the index starts from 1
                    }
                }
            })
            .withBroadcastSet(qualifiedItems, "qualifiedItems");

        // Map each sequences to an int array. We use 0 to separate elements.
        DataSet<int[]> sequences = inputSequences
            .map(new RichMapFunction<List<List<String>>, int[]>() {
                transient Map<String, Integer> tokenToId;

                @Override
                public void open(Configuration parameters) throws Exception {
                    tokenToId = new HashMap<>();
                    List<Tuple2<String, Integer>> bc = getRuntimeContext().getBroadcastVariable("itemIndex");
                    bc.forEach(t -> tokenToId.put(t.f0, t.f1));
                }

                @Override
                public int[] map(List<List<String>> elements) throws Exception {
                    List<Integer> seq = new ArrayList<>();
                    seq.add(0);
                    for (List<String> element : elements) {
                        int cnt = 0;
                        for (String it : element) {
                            Integer id = tokenToId.get(it);
                            if (id != null) {
                                cnt++;
                                seq.add(id);
                            }
                        }
                        if (cnt > 0) {
                            seq.add(0);
                        }
                    }
                    int[] sequence = new int[seq.size()];
                    for (int i = 0; i < sequence.length; i++) {
                        sequence[i] = seq.get(i);
                    }
                    return sequence;
                }
            })
            .withBroadcastSet(itemIndex, "itemIndex")
            .name("map_seq_to_int_array");

        DataSet<Tuple2<Integer, Integer>> qualifiedItemCount = itemCounts.join(itemIndex)
            .where(0).equalTo(0).projectSecond(1).projectFirst(1);

        ParallelPrefixSpan ps = new ParallelPrefixSpan(sequences, minSupportCnt, qualifiedItemCount, maxPatternLength);
        DataSet<Tuple2<int[], Integer>> freqPatterns = ps.run();

        DataSet<Tuple4<int[], int[], Integer, double[]>> rules =
            SequenceRule.extractSequenceRules(freqPatterns, sequenceCount, minConfidence);

        // Maps the indices in freq patterns and rules back to the original strings.
        DataSet<Row> patternsOutput = patternsIndexToString(freqPatterns, itemIndex);
        DataSet<Row> rulesOutput = rulesIndexToString(rules, itemIndex);

        Table table0 = DataSetConversionUtil.toTable(getMLEnvironmentId(), patternsOutput, ITEMSETS_COL_NAMES, ITEMSETS_COL_TYPES);
        Table table1 = DataSetConversionUtil.toTable(getMLEnvironmentId(), rulesOutput, RULES_COL_NAMES, RULES_COL_TYPES);

        this.setOutputTable(table0);
        this.setSideOutputTables(new Table[]{
            table1
        });
        return this;
    }

    /**
     * Count number of records in the dataset.
     *
     * @return a dataset of one record, recording the number of records of "dataSet".
     */
    private static <T> DataSet<Long> count(DataSet<T> dataSet) {
        return dataSet
            .mapPartition(new MapPartitionFunction<T, Long>() {
                @Override
                public void mapPartition(Iterable<T> values, Collector<Long> out) throws Exception {
                    long cnt = 0L;
                    for (T v : values) {
                        cnt++;
                    }
                    out.collect(cnt);
                }
            })
            .name("count_dataset")
            .returns(Types.LONG)
            .reduce(new ReduceFunction<Long>() {
                @Override
                public Long reduce(Long value1, Long value2) throws Exception {
                    return value1 + value2;
                }
            });
    }

    private static DataSet<Long>
    getMinSupportCnt(DataSet<Long> transactionsCnt,
                     final int minSupportCount, final double minSupportPercent) {
        return transactionsCnt.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                if (minSupportCount >= 0) {
                    return (long) minSupportCount;
                } else {
                    return (long) (Math.floor(value * minSupportPercent));
                }
            }
        });
    }

    /**
     * Encode the sequence patterns.
     */
    private static Tuple3<String, Long, Long> encodeSequence(int[] sequence, String[] indexToString) {
        StringBuilder sbd = new StringBuilder();
        int itemSetSize = 0;
        long chainLength = 1L;
        long itemCount = 0L;
        for (int i = 1; i < sequence.length - 1; i++) {
            if (sequence[i] == 0) {
                sbd.append(ELEMENT_SEPARATOR);
                chainLength++;
                itemSetSize = 0;
            } else {
                if (itemSetSize > 0) {
                    sbd.append(ITEM_SEPARATOR);
                }
                sbd.append(indexToString[sequence[i]]);
                itemSetSize++;
                itemCount++;
            }
        }
        return Tuple3.of(sbd.toString(), itemCount, chainLength);
    }

    /**
     * Maps items' ids to strings in frequent patterns.
     *
     * @param patterns  A dataset of: frequent patterns (represented as int array), support count.
     * @param itemIndex A dataset which is a mapping from items' names to indices.
     * @return A dataset of frequent patterns that is for output.
     */
    private static DataSet<Row> patternsIndexToString(DataSet<Tuple2<int[], Integer>> patterns,
                                                      DataSet<Tuple2<String, Integer>> itemIndex) {
        return patterns
            .map(new RichMapFunction<Tuple2<int[], Integer>, Row>() {
                transient String[] itemNames;

                @Override
                public void open(Configuration parameters) throws Exception {
                    List<Tuple2<String, Integer>> bc = getRuntimeContext().getBroadcastVariable("itemIndex");
                    itemNames = new String[bc.size() + 1];
                    bc.forEach(t -> itemNames[t.f1] = t.f0);
                }

                @Override
                public Row map(Tuple2<int[], Integer> value) throws Exception {
                    int[] sequence = value.f0;
                    Tuple3<String, Long, Long> encoded = encodeSequence(sequence, itemNames);
                    return Row.of(encoded.f0, value.f1.longValue(), encoded.f1);
                }
            })
            .withBroadcastSet(itemIndex, "itemIndex")
            .name("patternsIndexToString");
    }

    /**
     * Maps items' ids to strings in association rules.
     *
     * @param rules     A dataset of: antecedent, consequent, support count, [lift, support, confidence].
     * @param itemIndex A dataset which is a mapping from items' names to indices.
     * @return A dataset of association rules that is for output.
     */
    private static DataSet<Row> rulesIndexToString(DataSet<Tuple4<int[], int[], Integer, double[]>> rules,
                                                   DataSet<Tuple2<String, Integer>> itemIndex) {
        return rules
            .map(new RichMapFunction<Tuple4<int[], int[], Integer, double[]>, Row>() {
                transient String[] itemNames;

                @Override
                public void open(Configuration parameters) throws Exception {
                    List<Tuple2<String, Integer>> bc = getRuntimeContext().getBroadcastVariable("itemIndex");
                    itemNames = new String[bc.size() + 1];
                    bc.forEach(t -> {
                        itemNames[t.f1] = t.f0;
                    });
                }

                @Override
                public Row map(Tuple4<int[], int[], Integer, double[]> value) throws Exception {
                    Tuple3<String, Long, Long> antecedent = encodeSequence(value.f0, this.itemNames);
                    Tuple3<String, Long, Long> consequent = encodeSequence(value.f1, this.itemNames);
                    return Row.of(antecedent.f0 + RULE_SEPARATOR + consequent.f0,
                        antecedent.f2 + consequent.f2, value.f3[0], value.f3[1], value.f2.longValue());
                }
            })
            .withBroadcastSet(itemIndex, "itemIndex")
            .name("rulesIndexToString");
    }
}
