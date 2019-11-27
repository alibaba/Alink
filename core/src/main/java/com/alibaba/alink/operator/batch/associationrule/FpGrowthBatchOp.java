package com.alibaba.alink.operator.batch.associationrule;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.associationrule.AssociationRule;
import com.alibaba.alink.operator.common.associationrule.FpTree;
import com.alibaba.alink.operator.common.associationrule.ParallelFpGrowth;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.associationrule.FpGrowthParams;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
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


/**
 * FpGrowth computes frequent itemsets given a set of transactions.
 * The FP-Growth algorithm is described in <a href="http://dx.doi.org/10.1145/335191.335372">
 * Han et al., Mining frequent patterns without candidate generation</a>.
 */
public final class FpGrowthBatchOp
    extends BatchOperator<FpGrowthBatchOp>
    implements FpGrowthParams<FpGrowthBatchOp> {

    private static final Logger LOG = LoggerFactory.getLogger(FpGrowthBatchOp.class);

    /**
     * Separator between items in a transaction.
     */
    public static final String ITEM_SEPARATOR = ",";

    static final String[] ITEMSETS_COL_NAMES = new String[]{"itemset", "supportcount", "itemcount"};

    static final String[] RULES_COL_NAMES = new String[]{"rule", "itemcount", "lift",
        "support_percent", "confidence_percent", "transaction_count"};

    static final TypeInformation[] ITEMSETS_COL_TYPES = new TypeInformation[]{
        Types.STRING, Types.LONG, Types.LONG};

    static final TypeInformation[] RULES_COL_TYPES = new TypeInformation[]{
        Types.STRING, Types.LONG, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.LONG};

    private FpTree fpTree;

    public FpGrowthBatchOp() {
        this(new Params());
    }

    public FpGrowthBatchOp(Params params) {
        super(params);
    }

    @Override
    public FpGrowthBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        final String itemsColName = getItemsCol();
        final int minSupportCount = getMinSupportCount();
        final double minSupportPercent = getMinSupportPercent();
        final double minConfidence = getMinConfidence();
        final int maxConsequentLength = getMaxConsequentLength();
        final double minLift = getMinLift();
        final int maxPatternLength = getMaxPatternLength();
        final int itemsColIdx = TableUtil.findColIndex(in.getSchema(), itemsColName);

        Preconditions.checkArgument(itemsColIdx >= 0, "Can't find column: " + itemsColName);

        DataSet<Set<String>> itemsets = ((DataSet<Row>) in.getDataSet())
            .map(new MapFunction<Row, Set<String>>() {
                @Override
                public Set<String> map(Row value) throws Exception {
                    Set<String> itemset = new HashSet<>();
                    String itemsetStr = (String) value.getField(itemsColIdx);
                    if (!StringUtils.isNullOrWhitespaceOnly(itemsetStr)) {
                        String[] splited = itemsetStr.split(ITEM_SEPARATOR);
                        itemset.addAll(Arrays.asList(splited));
                    }
                    return itemset;
                }
            });

        DataSet<Long> transactionsCnt = count(itemsets);
        DataSet<Long> minSupportCnt = getMinSupportCnt(transactionsCnt, minSupportCount, minSupportPercent);

        // Count the support of each items.
        DataSet<Tuple2<String, Integer>> itemCounts = itemsets
            .mapPartition(new MapPartitionFunction<Set<String>, Tuple2<String, Integer>>() {
                @Override
                public void mapPartition(Iterable<Set<String>> transactions, Collector<Tuple2<String, Integer>> out) throws Exception {
                    Map<String, Integer> localCounts = new HashMap<>();
                    for (Set<String> items : transactions) {
                        for (String item : items) {
                            localCounts.merge(item, 1, Integer::sum);
                        }
                    }
                    localCounts.forEach((k, v) -> {
                        out.collect(Tuple2.of(k, v));
                    });
                }
            })
            .groupBy(0)
            .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                    value1.f1 += value2.f1;
                    return value1;
                }
            });

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
            .name("getQualifiedItems");

        // Assign items with indices, ordered by their support.
        DataSet<Tuple2<String, Integer>> itemIndex = itemsets.getExecutionEnvironment().fromElements(0)
            .flatMap(new RichFlatMapFunction<Integer, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(Integer value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    List<Tuple2<String, Integer>> bc = getRuntimeContext().getBroadcastVariable("qualifiedItems");
                    Integer[] order = new Integer[bc.size()];
                    for (int i = 0; i < order.length; i++) {
                        order[i] = i;
                    }
                    Arrays.sort(order, new Comparator<Integer>() {
                        @Override
                        public int compare(Integer o1, Integer o2) {
                            Integer cnt1 = bc.get(o1).f1;
                            Integer cnt2 = bc.get(o2).f1;
                            if (cnt1.equals(cnt2)) {
                                return bc.get(o1).f0.compareTo(bc.get(o2).f0);
                            }
                            return Integer.compare(cnt2, cnt1);
                        }
                    });
                    for (int i = 0; i < order.length; i++) {
                        out.collect(Tuple2.of(bc.get(order[i]).f0, i));
                    }
                }
            })
            .withBroadcastSet(qualifiedItems, "qualifiedItems");

        // Map each transactions to an int array.
        DataSet<int[]> transactions = itemsets
            .map(new RichMapFunction<Set<String>, int[]>() {
                transient Map<String, Integer> tokenToId;

                @Override
                public void open(Configuration parameters) throws Exception {
                    tokenToId = new HashMap<>();
                    List<Tuple2<String, Integer>> bc = getRuntimeContext().getBroadcastVariable("itemIndex");
                    bc.forEach(t -> tokenToId.put(t.f0, t.f1));
                }

                @Override
                public int[] map(Set<String> itemset) throws Exception {
                    int[] items = new int[itemset.size()];
                    int len = 0;
                    for (String item : itemset) {
                        Integer id = tokenToId.get(item);
                        if (id != null) {
                            items[len++] = id;
                        }
                    }
                    if (len > 0) {
                        int[] qualified = Arrays.copyOfRange(items, 0, len);
                        Arrays.sort(qualified);
                        return qualified;
                    } else {
                        return new int[0];
                    }
                }
            })
            .withBroadcastSet(itemIndex, "itemIndex");

        DataSet<Tuple2<Integer, Integer>> qualifiedItemCount = itemCounts.join(itemIndex)
            .where(0).equalTo(0).projectSecond(1).projectFirst(1);

        int numPartitions = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().getParallelism();
        ParallelFpGrowth parallelFpGrowth = new ParallelFpGrowth(this.fpTree, transactions, qualifiedItemCount, minSupportCnt, maxPatternLength, numPartitions);
        DataSet<Tuple2<int[], Integer>> patterns = parallelFpGrowth.run();
        DataSet<Row> itemsetsOutput = patternsIndexToString(patterns, itemIndex);

        DataSet<Tuple4<int[], int[], Integer, double[]>> rules =
            AssociationRule.extractRules(patterns, transactionsCnt, qualifiedItemCount, minConfidence, minLift, maxConsequentLength);
        DataSet<Row> rulesOutput = rulesIndexToString(rules, itemIndex);

        Table patternsTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), itemsetsOutput, ITEMSETS_COL_NAMES, ITEMSETS_COL_TYPES);
        Table rulesTable = DataSetConversionUtil.toTable(getMLEnvironmentId(), rulesOutput, RULES_COL_NAMES, RULES_COL_TYPES);

        this.setOutputTable(patternsTable);
        this.setSideOutputTables(new Table[]{rulesTable});
        return this;

    }

    /**
     * Set the local FP tree implementation. This allows custom implementation provided by users.
     */
    public void setFpTree(FpTree fpTree) {
        this.fpTree = fpTree;
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

    private static String concatItems(String[] itemNames, int[] items) {
        StringBuilder sbd = new StringBuilder();
        for (int i = 0; i < items.length; i++) {
            if (i > 0) {
                sbd.append(ITEM_SEPARATOR);
            }
            sbd.append(itemNames[items[i]]);
        }
        return sbd.toString();
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
                    itemNames = new String[bc.size()];
                    bc.forEach(t -> itemNames[t.f1] = t.f0);
                }

                @Override
                public Row map(Tuple2<int[], Integer> value) throws Exception {
                    return Row.of(concatItems(itemNames, value.f0), value.f1.longValue(), (long) (value.f0.length));
                }
            })
            .withBroadcastSet(itemIndex, "itemIndex");
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
                    itemNames = new String[bc.size()];
                    bc.forEach(t -> {
                        itemNames[t.f1] = t.f0;
                    });
                }

                @Override
                public Row map(Tuple4<int[], int[], Integer, double[]> value) throws Exception {
                    long itemCount = value.f0.length + value.f1.length;
                    String antecedent = concatItems(itemNames, value.f0);
                    String consequent = concatItems(itemNames, value.f1);
                    return Row.of(antecedent + "=>" + consequent, itemCount, value.f3[0], value.f3[1], value.f3[2], value.f2.longValue());
                }
            })
            .withBroadcastSet(itemIndex, "itemIndex");
    }

}
