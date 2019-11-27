package com.alibaba.alink.operator.common.associationrule;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * The class for generating association rules given the frequent patterns.
 */
public class AssociationRule {

    /**
     * Generate association rules from frequent patterns.
     *
     * @param patterns            A DataSet of frequent patterns and there supports.
     * @param transactionsCnt     The number of transactions in the original dataset.
     * @param itemCounts          A DataSet of items and their supports.
     * @param minConfidence       Minimum confidence.
     * @param minLift             Minimum lift.
     * @param maxConsequentLength Maximum length of a consequent.
     * @return The association rules with fields: antecedent(left hand side), consequent(right hand side),
     * support count, [lift, support, confidence]).
     */
    public static DataSet<Tuple4<int[], int[], Integer, double[]>> extractRules(
        DataSet<Tuple2<int[], Integer>> patterns,
        DataSet<Long> transactionsCnt,
        DataSet<Tuple2<Integer, Integer>> itemCounts,
        final double minConfidence,
        final double minLift,
        final int maxConsequentLength) {

        if (maxConsequentLength <= 0) {
            return patterns.getExecutionEnvironment().fromElements(0)
                .flatMap(new FlatMapFunction<Integer, Tuple4<int[], int[], Integer, double[]>>() {
                    @Override
                    public void flatMap(Integer value, Collector<Tuple4<int[], int[], Integer, double[]>> out) throws Exception {
                    }
                });
        } else if (maxConsequentLength == 1) {
            return extractSingleConsequentRules(patterns, transactionsCnt, itemCounts, minConfidence, minLift);
        } else {
            return extractMultiConsequentRules(patterns, transactionsCnt, minConfidence, minLift,
                maxConsequentLength);
        }
    }

    /**
     * Extract association rules for the special case that the maximum consequent length is one.
     */
    private static DataSet<Tuple4<int[], int[], Integer, double[]>> extractSingleConsequentRules(
        DataSet<Tuple2<int[], Integer>> patterns,
        DataSet<Long> transactionsCnt,
        DataSet<Tuple2<Integer, Integer>> itemCounts,
        final double minConfidence,
        final double minLift) {

        // A dataset of: pattern, support, tail, len, rotation flag
        DataSet<Tuple5<int[], Integer, Integer, Integer, Boolean>> rotatedItemSets = patterns
            .flatMap(new RichFlatMapFunction<Tuple2<int[], Integer>, Tuple5<int[], Integer, Integer, Integer, Boolean>>() {
                @Override
                public void flatMap(Tuple2<int[], Integer> value, Collector<Tuple5<int[], Integer, Integer, Integer, Boolean>> out) throws Exception {
                    int[] items = value.f0;
                    out.collect(Tuple5.of(value.f0, value.f1, items[items.length - 1], items.length, false));
                    if (items.length <= 1) {
                        return;
                    }
                    int tail = items[items.length - 1];
                    for (int i = items.length - 1; i >= 1; i--) {
                        items[i] = items[i - 1];
                    }
                    items[0] = tail;
                    out.collect(Tuple5.of(items, value.f1, items[items.length - 1], items.length, true));
                }
            })
            .withBroadcastSet(itemCounts, "itemCounts");

        // Group the dataset by the tail of each patterns, then all association rules can be extracted within each group.
        // Get a dataset of: antecedent, consequent, support count, [lift, support, confidence]
        return rotatedItemSets
            .groupBy(2)
            .sortGroup(3, Order.ASCENDING)
            .reduceGroup(new RichGroupReduceFunction<Tuple5<int[], Integer, Integer, Integer, Boolean>,
                Tuple4<int[], int[], Integer, double[]>>() {
                transient Map<Integer, Integer> itemCounts;
                transient double transactionCount;
                transient Map<int[], Integer> supportMap;

                @Override
                public void open(Configuration parameters) throws Exception {
                    itemCounts = new HashMap<>();
                    List<Tuple2<Integer, Integer>> bcItemCounts = getRuntimeContext().getBroadcastVariable("itemCounts");
                    List<Long> bcTransactionCount = getRuntimeContext().getBroadcastVariable("transactionsCnt");
                    bcItemCounts.forEach(t2 -> {
                        itemCounts.put(t2.f0, t2.f1);
                    });
                    transactionCount = bcTransactionCount.get(0).doubleValue();
                }

                @Override
                public void reduce(Iterable<Tuple5<int[], Integer, Integer, Integer, Boolean>> patterns,
                                   Collector<Tuple4<int[], int[], Integer, double[]>> out) throws Exception {
                    supportMap = new TreeMap<>((o1, o2) -> {
                        if (o1.length != o2.length) {
                            return Integer.compare(o1.length, o2.length);
                        }
                        for (int i = 0; i < o1.length; i++) {
                            if (o1[i] != o2[i]) {
                                return Integer.compare(o1[i], o2[i]);
                            }
                        }
                        return 0;
                    });

                    for (Tuple5<int[], Integer, Integer, Integer, Boolean> pattern : patterns) {
                        boolean rotated = pattern.f4;
                        int[] items = pattern.f0;
                        if (rotated) {
                            int[] ante = Arrays.copyOfRange(items, 1, items.length);
                            int conseq = items[0];
                            exportRule(ante, conseq, pattern.f1, out);
                        } else {
                            for (int i = 0; i < items.length - 1; i++) {
                                int[] ante = new int[items.length - 1];
                                System.arraycopy(items, 0, ante, 0, i);
                                System.arraycopy(items, i + 1, ante, i, items.length - i - 1);
                                int conseq = items[i];
                                exportRule(ante, conseq, pattern.f1, out);
                            }
                            supportMap.put(items, pattern.f1);
                        }
                    }
                }

                private void exportRule(int[] x, int y, int suppXY, Collector<Tuple4<int[], int[], Integer, double[]>> out) {
                    Integer suppX = supportMap.get(x);
                    Integer suppY = itemCounts.get(y);
                    assert suppX != null && suppY != null;
                    assert suppX >= suppXY && suppY >= suppXY;
                    double lift = suppXY * transactionCount / (suppX.doubleValue() * suppY.doubleValue());
                    double confidence = suppXY / suppX.doubleValue();
                    double support = suppXY / transactionCount;
                    if (lift >= minLift && confidence >= minConfidence) {
                        out.collect(Tuple4.of(x, new int[]{y}, suppXY, new double[]{lift, support, confidence}));
                    }
                }
            })
            .withBroadcastSet(itemCounts, "itemCounts")
            .withBroadcastSet(transactionsCnt, "transactionsCnt")
            .name("extract_rules");
    }

    /**
     * Extract association rules for maximum consequent length larger than one.
     */
    private static DataSet<Tuple4<int[], int[], Integer, double[]>> extractMultiConsequentRules(
        DataSet<Tuple2<int[], Integer>> patterns,
        DataSet<Long> transactionsCnt,
        final double minConfidence,
        final double minLift,
        final int maxConsequentLength) {

        // tuple: antecedent(x), consequent(y), support(xy)
        DataSet<Tuple3<int[], int[], Integer>> candidates = patterns
            .flatMap(new FlatMapFunction<Tuple2<int[], Integer>, Tuple3<int[], int[], Integer>>() {
                @Override
                public void flatMap(Tuple2<int[], Integer> value, Collector<Tuple3<int[], int[], Integer>> out)
                    throws Exception {
                    int[] itemset = value.f0;
                    if (itemset.length <= 1) {
                        return;
                    }
                    for (int i = 1; i <= maxConsequentLength; i++) {
                        if (itemset.length <= i) {
                            continue;
                        }
                        List<int[]> comb = combination(itemset.length, i);
                        int[] antecedent = new int[itemset.length - i];
                        int[] consequent = new int[i];
                        comb.forEach(t -> {
                            int len = t.length;
                            int pos0 = 0;
                            int pos1 = 0;
                            for (int j = 0; j < len; j++) {
                                if (t[j] == 1) {
                                    consequent[pos0++] = itemset[j];
                                } else {
                                    antecedent[pos1++] = itemset[j];
                                }
                            }
                            out.collect(Tuple3.of(antecedent, consequent, value.f1));
                        });
                    }
                }
            })
            .name("gen_rules_candidates");

        // tuple: antecedent(x), consequent(y), support(xy), support(x)
        DataSet<Tuple4<int[], int[], Integer, Integer>> candidatesSupport = candidates
            .join(patterns)
            .where(0).equalTo(0)
            .projectFirst(0, 1, 2).projectSecond(1);

        // tuple: antecedent(x), consequent(y), support(xy), support(x)
        DataSet<Tuple4<int[], int[], Integer, Integer>> qualified = candidatesSupport
            .filter(new FilterFunction<Tuple4<int[], int[], Integer, Integer>>() {
                @Override
                public boolean filter(Tuple4<int[], int[], Integer, Integer> value) throws Exception {
                    Integer supportXY = value.f2;
                    Integer supportX = value.f3;
                    assert supportX >= supportXY;
                    double confidence = supportXY.doubleValue() / supportX.doubleValue();
                    return confidence >= minConfidence;
                }
            })
            .withBroadcastSet(transactionsCnt, "transactionsCnt")
            .name("filter_rules");

        // tuple: antecedent(x), consequent(y), support(xy), support(x), support(y)
        DataSet<Tuple5<int[], int[], Integer, Integer, Integer>> rules;

        rules = qualified
            .join(patterns)
            .where(1).equalTo(0)
            .projectFirst(0, 1, 2, 3).projectSecond(1);

        return rules
            .flatMap(
                new RichFlatMapFunction<Tuple5<int[], int[], Integer, Integer, Integer>, Tuple4<int[], int[], Integer, double[]>>() {
                    transient Long transactionsCnt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        List<Long> bc = getRuntimeContext().getBroadcastVariable("transactionsCnt");
                        this.transactionsCnt = bc.get(0);
                    }

                    @Override
                    public void flatMap(Tuple5<int[], int[], Integer, Integer, Integer> value,
                                        Collector<Tuple4<int[], int[], Integer, double[]>> out) throws Exception {
                        Integer supportXY = value.f2;
                        Integer supportX = value.f3;
                        Integer supportY = value.f4;
                        double lift = supportXY.doubleValue() * transactionsCnt.doubleValue() / (supportX.doubleValue()
                            * supportY.doubleValue());
                        if (lift >= minLift) {
                            double confidence = supportXY.doubleValue() / supportX.doubleValue();
                            double support = supportXY.doubleValue() / transactionsCnt.doubleValue();
                            out.collect(
                                Tuple4.of(value.f0, value.f1, value.f2, new double[]{lift, support, confidence}));
                        }
                    }
                })
            .withBroadcastSet(transactionsCnt, "transactionsCnt")
            .name("output_rules");
    }

    /**
     * Enumerate all combinations of items of size n given a total number of m items. The picked items are
     * marked with 1, otherwise 0.
     */
    private static List<int[]> combination(int m, int n) {
        return combination(m, n, m);
    }

    /**
     * Enumerate all combinations of items of size n from the first m items out of a total number of l items.
     */
    private static List<int[]> combination(int m, int n, int l) {
        if (m < n) {
            return new ArrayList<>();
        } else if (m == n) {
            List<int[]> ret = new ArrayList<>(1);
            int[] c = new int[l];
            Arrays.fill(c, 0, m, 1);
            ret.add(c);
            return ret;
        } else {
            if (n <= 0) {
                return new ArrayList<>();
            } else if (n == 1) {
                List<int[]> ret = new ArrayList<>(m);
                for (int i = 0; i < m; i++) {
                    int[] c = new int[l];
                    c[m - i - 1] = 1;
                    ret.add(c);
                }
                return ret;
            } else {
                List<int[]> ret = new ArrayList<>();
                for (int i = m - 1; i >= n - 1; i--) {
                    List<int[]> sub = combination(i, n - 1, l);
                    for (int[] aSub : sub) {
                        aSub[i] = 1;
                    }
                    ret.addAll(sub);
                }
                return ret;
            }
        }
    }
}