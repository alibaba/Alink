package com.alibaba.alink.operator.common.associationrule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * An implementation of parallel FP Growth algorithm.
 * <p>
 * In this implementation, the items are first partitioned to each workers, then each of the workers
 * receives their conditional db, with which frequent patterns are generated therein.
 * <p>
 * For detail descriptions, please refer to:
 * "Li et al., PFP: Parallel FP-growth for query recommendation".
 */
public class ParallelFpGrowth {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelFpGrowth.class);

    private DataSet<int[]> transactions;
    private final DataSet<Tuple2<Integer, Integer>> itemCounts;
    private DataSet<Long> minSupportCnt;
    private final FpTree fpTree;
    private final int maxPatternLength;
    private final int numPartitions;

    /**
     * The constructor.
     *
     * @param fpTree           The implementation of local FP Growth algorithm.
     * @param transactions     A DataSet of transactions. Items in each transactions should be ordered descending by their supports.
     * @param itemCounts       A DataSet of items and their supports.
     * @param minSupportCnt    Minimum support.
     * @param maxPatternLength Maximum pattern length.
     * @param numPartitions    Number of partitions.
     */
    public ParallelFpGrowth(@Nullable FpTree fpTree, DataSet<int[]> transactions,
                            DataSet<Tuple2<Integer, Integer>> itemCounts,
                            DataSet<Long> minSupportCnt, int maxPatternLength, int numPartitions) {
        if (fpTree == null) {
            this.fpTree = new FpTreeImpl();
        } else {
            this.fpTree = fpTree;
        }
        this.transactions = transactions;
        this.minSupportCnt = minSupportCnt;
        this.maxPatternLength = maxPatternLength;
        this.numPartitions = numPartitions;
        this.itemCounts = itemCounts;
    }

    /**
     * Generate frequent patterns.
     *
     * @return A DataSet of frequent patterns and their supports.
     */
    public DataSet<Tuple2<int[], Integer>> run() {
        DataSet<Tuple3<Integer, Integer, Integer>> partitioner = partitionItems(itemCounts, numPartitions);
        DataSet<Tuple2<Integer, int[]>> condTransactions = genCondTransactions(transactions, partitioner, numPartitions);
        return mineFreqItemsets(condTransactions, partitioner.project(2, 0),
            minSupportCnt, maxPatternLength, this.fpTree);
    }

    /**
     * Generate items partition. To achieve load balance, we assign to each item a score that
     * represents its estimation of number of nodes in the Fp-tree. Then we greedily partition
     * the items to balance the sum of scores in each partitions.
     *
     * @param itemCounts    Support count of each item.
     * @param numPartitions Number of data partitions.
     * @return A DataSet of tuples of item, order and partition
     */
    private static DataSet<Tuple3<Integer, Integer, Integer>> partitionItems(
        DataSet<Tuple2<Integer, Integer>> itemCounts, final int numPartitions) {

        return itemCounts
            .mapPartition(new RichMapPartitionFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                @Override
                public void mapPartition(Iterable<Tuple2<Integer, Integer>> values,
                                         Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
                    List<Tuple2<Integer, Integer>> itemCounts = new ArrayList<>();
                    for (Tuple2<Integer, Integer> v : values) {
                        itemCounts.add(v);
                    }
                    itemCounts.sort((o1, o2) -> {
                        int cmp = Long.compare(o2.f1, o1.f1);
                        return cmp == 0 ? Integer.compare(o1.f0, o2.f0) : cmp;
                    });

                    // queue of tuple: partition, count
                    PriorityQueue<Tuple2<Integer, Double>> queue = new PriorityQueue<>(numPartitions,
                        Comparator.comparingDouble(o -> o.f1));

                    for (int i = 0; i < numPartitions; i++) {
                        queue.add(Tuple2.of(i, 0.0));
                    }

                    List<Double> scaledItemCount = new ArrayList<>(itemCounts.size());
                    for (int i = 0; i < itemCounts.size(); i++) {
                        Tuple2<Integer, Integer> item = itemCounts.get(i);
                        double pos = (double) i / ((double) itemCounts.size());
                        double score = pos * item.f1.doubleValue();
                        scaledItemCount.add(score);
                    }

                    List<Integer> order = new ArrayList<>(itemCounts.size());
                    for (int i = 0; i < itemCounts.size(); i++) {
                        order.add(i);
                    }

                    order.sort((o1, o2) -> {
                        double s1 = scaledItemCount.get(o1);
                        double s2 = scaledItemCount.get(o2);
                        return Double.compare(s2, s1);
                    });

                    // greedily assign partition number to each item
                    for (int i = 0; i < itemCounts.size(); i++) {
                        Tuple2<Integer, Integer> item = itemCounts.get(order.get(i));
                        double score = scaledItemCount.get(order.get(i));
                        Tuple2<Integer, Double> target = queue.poll();
                        int targetPartition = target.f0;
                        target.f1 += score;
                        queue.add(target);
                        out.collect(Tuple3.of(item.f0, order.get(i), targetPartition));
                    }
                }
            })
            .setParallelism(1)
            .name("create_partitioner");
    }

    /**
     * Generate conditional transactions for each partitions.
     *
     * @param transactions  A DataSet of transactions.
     * @param partitioner   A DataSet of tuples of item, order and partition number.
     * @param numPartitions Number of partitions.
     * @return A DataSet of tuples of partition number and conditional transaction.
     */
    private static DataSet<Tuple2<Integer, int[]>>
    genCondTransactions(DataSet<int[]> transactions, DataSet<Tuple3<Integer, Integer, Integer>> partitioner,
                        final int numPartitions) {
        return transactions
            .flatMap(new RichFlatMapFunction<int[], Tuple2<Integer, int[]>>() {
                final int ITEM_PARTITION = 1;
                transient Map<Integer, Tuple2<Integer, Integer>> itemPartitioner;
                transient int[] flags;

                @Override
                public void open(Configuration parameters) throws Exception {
                    this.itemPartitioner = getRuntimeContext()
                        .getBroadcastVariableWithInitializer("partitioner",
                            new BroadcastVariableInitializer<Tuple3<Integer, Integer, Integer>, Map<Integer, Tuple2
                                                            <Integer, Integer>>>() {
                                @Override
                                public Map<Integer, Tuple2<Integer, Integer>> initializeBroadcastVariable(
                                    Iterable<Tuple3<Integer, Integer, Integer>> data) {
                                    Map<Integer, Tuple2<Integer, Integer>> itemPartitioner = new HashMap<>();
                                    for (Tuple3<Integer, Integer, Integer> item : data) {
                                        itemPartitioner.put(item.f0, Tuple2.of(item.f1, item.f2));
                                    }
                                    return itemPartitioner;
                                }
                            });
                    this.flags = new int[numPartitions];
                }

                @Override
                public void flatMap(int[] transaction, Collector<Tuple2<Integer, int[]>> out) throws Exception {
                    Arrays.fill(flags, 0);
                    int cnt = transaction.length;
                    for (int i = 0; i < cnt; i++) {
                        int lastPos = cnt - i;
                        int partition = this.itemPartitioner.get(transaction[lastPos - 1]).getField(ITEM_PARTITION);
                        if (flags[partition] == 0) {
                            List<Integer> condTransaction = new ArrayList<>(lastPos);
                            for (int j = 0; j < lastPos; j++) {
                                condTransaction.add(transaction[j]);
                            }
                            int[] tr = new int[condTransaction.size()];
                            for (int j = 0; j < tr.length; j++) {
                                tr[j] = condTransaction.get(j);
                            }
                            out.collect(Tuple2.of(partition, tr));
                            flags[partition] = 1;
                        }
                    }
                }
            })
            .withBroadcastSet(partitioner, "partitioner")
            .name("gen_cond_transactions");
    }

    /**
     * Mine frequent patterns locally in each partitions.
     *
     * @param condTransactions The conditional transactions with partition id.
     * @param partitioner      A DataSet of partition id and item.
     * @param minSupportCnt    Minimum support.
     * @param maxLength        Maximum pattern length.
     * @param tree             Local FP Growth algorithm.
     * @return A DataSet of frequent patterns with support.
     */
    private static DataSet<Tuple2<int[], Integer>> mineFreqItemsets(
        DataSet<Tuple2<Integer, int[]>> condTransactions, DataSet<Tuple2<Integer, Integer>> partitioner,
        DataSet<Long> minSupportCnt, final int maxLength, final FpTree tree) {
        return condTransactions
            .coGroup(partitioner)
            .where(0).equalTo(0)
            .withPartitioner(new Partitioner<Integer>() {
                @Override
                public int partition(Integer key, int numPartitions) {
                    return key % numPartitions;
                }
            })
            .with(new RichCoGroupFunction<Tuple2<Integer, int[]>, Tuple2<Integer, Integer>, Tuple2<int[], Integer>>() {
                @Override
                public void coGroup(Iterable<Tuple2<Integer, int[]>> transactions,
                                    Iterable<Tuple2<Integer, Integer>> items,
                                    Collector<Tuple2<int[], Integer>> out) throws Exception {
                    List<Long> bc = getRuntimeContext().getBroadcastVariable("minSupportCnt");
                    long minSupportCnt = bc.get(0);
                    LOG.info("minSupportCnt = {}", minSupportCnt);

                    long t0 = System.currentTimeMillis();
                    tree.createTree();
                    for (Tuple2<Integer, int[]> transaction : transactions) {
                        tree.addTransaction(transaction.f1);
                    }

                    tree.initialize();
                    tree.printProfile();

                    List<Tuple2<Integer, Integer>> cachedItems = new ArrayList<>();
                    items.forEach(cachedItems::add);
                    int[] suffices = new int[cachedItems.size()];
                    for (int i = 0; i < suffices.length; i++) {
                        suffices[i] = cachedItems.get(i).f1;
                    }
                    tree.extractAll(suffices, (int) minSupportCnt, maxLength, out);

                    tree.destroyTree();
                    long t1 = System.currentTimeMillis();
                    LOG.info("Done local FpGrowth in {}s.", (t1 - t0) / 1000L);
                }
            })
            .withBroadcastSet(minSupportCnt, "minSupportCnt")
            .name("fpgrowth")
            .map(new MapFunction<Tuple2<int[], Integer>, Tuple2<int[], Integer>>() {
                @Override
                public Tuple2<int[], Integer> map(Tuple2<int[], Integer> value) throws Exception {
                    int[] pattern = value.f0;
                    Arrays.sort(pattern);
                    return Tuple2.of(pattern, value.f1);
                }
            });
    }
}