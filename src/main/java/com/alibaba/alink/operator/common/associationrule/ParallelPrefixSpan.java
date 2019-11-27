package com.alibaba.alink.operator.common.associationrule;

import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Implementation of the PrefixSpan algorithm.
 * <p>
 * The PrefixSpan algorithm is described in J. Pei, et al.,
 * Mining Sequential Patterns by Pattern-Growth: The PrefixSpan Approach
 */
public class ParallelPrefixSpan {
    private DataSet<int[]> sequences;
    private DataSet<Long> minSupportCnt;
    private DataSet<Tuple2<Integer, Integer>> itemCounts;
    private int maxPatternLength;

    /**
     * The constructor.
     *
     * @param sequences        A dataset of sequences. A sequence is represented as an int array, where 0 is
     *                         used to separate elements in a sequence. For example, [0,1,2,0,2,0,3,4,0] represents
     *                         a sequence with three elements "1,2", "2", and "3,4".
     * @param itemCounts       A DataSet of items and their supports.
     * @param minSupportCnt    Minimum support.
     * @param maxPatternLength Maximum pattern length.
     */
    public ParallelPrefixSpan(DataSet<int[]> sequences, DataSet<Long> minSupportCnt,
                              DataSet<Tuple2<Integer, Integer>> itemCounts, int maxPatternLength) {
        this.sequences = sequences;
        this.minSupportCnt = minSupportCnt;
        this.itemCounts = itemCounts;
        this.maxPatternLength = maxPatternLength;
    }

    /**
     * Generate frequent sequence patterns using PrefixSpan algorithm.
     *
     * @return Frequent sequence patterns and their supports.
     */
    public DataSet<Tuple2<int[], Integer>> run() {
        final int parallelism = BatchOperator.getExecutionEnvironmentFromDataSets(sequences).getParallelism();
        DataSet<Tuple2<Integer, int[]>> partitionedSequence = partitionSequence(sequences, itemCounts, parallelism);
        final int maxLength = maxPatternLength;

        return partitionedSequence
            .partitionCustom(new Partitioner<Integer>() {
                @Override
                public int partition(Integer key, int numPartitions) {
                    return key % numPartitions;
                }
            }, 0)
            .mapPartition(new RichMapPartitionFunction<Tuple2<Integer, int[]>, Tuple2<int[], Integer>>() {
                @Override
                public void mapPartition(Iterable<Tuple2<Integer, int[]>> values,
                                         Collector<Tuple2<int[], Integer>> out) throws Exception {
                    List<Long> bc1 = getRuntimeContext().getBroadcastVariable("minSupportCnt");
                    List<Tuple2<Integer, Integer>> bc2 = getRuntimeContext().getBroadcastVariable("itemCounts");
                    int taskId = getRuntimeContext().getIndexOfThisSubtask();

                    long minSuppCnt = bc1.get(0);
                    List<int[]> allSeq = new ArrayList<>();
                    values.forEach(t -> allSeq.add(t.f1));

                    List<Postfix> initialPostfixes = new ArrayList<>(allSeq.size());
                    for (int i = 0; i < allSeq.size(); i++) {
                        initialPostfixes.add(new Postfix(i));
                    }

                    bc2.forEach(itemCount -> {
                        int item = itemCount.f0;
                        if (item % parallelism == taskId) {
                            generateFreqPattern(allSeq, initialPostfixes, item, minSuppCnt, maxLength, out);
                        }
                    });
                }
            })
            .withBroadcastSet(this.minSupportCnt, "minSupportCnt")
            .withBroadcastSet(this.itemCounts, "itemCounts")
            .name("generate_freq_pattern");
    }

    /**
     * Distribute the sequences to each workers.
     * <p>
     * Each worker is assigned a subset of items, and the worker is responsible to compute all frequent
     * sequence patterns that start with those items.
     *
     * @param sequences     A dataset of sequences. In a sequence, 0 is element separator,
     *                      and a sequence always starts end ends with 0.
     * @param itemCounts    A DataSet of items and their supports.
     * @param numPartitions Number of partitions.
     * @return A dataset of partition no. and the sequence.
     */
    private static DataSet<Tuple2<Integer, int[]>>
    partitionSequence(DataSet<int[]> sequences, DataSet<Tuple2<Integer, Integer>> itemCounts, final int numPartitions) {
        return sequences
            .flatMap(new RichFlatMapFunction<int[], Tuple2<Integer, int[]>>() {
                transient Map<Integer, Integer> itemCounts;
                transient boolean[] flags;

                @Override
                public void open(Configuration parameters) throws Exception {
                    itemCounts = new HashMap<>();
                    List<Tuple2<Integer, Integer>> bc = getRuntimeContext().getBroadcastVariable("itemCounts");
                    bc.forEach(t -> itemCounts.put(t.f0, t.f1));
                    flags = new boolean[numPartitions];
                }

                @Override
                public void flatMap(int[] sequence, Collector<Tuple2<Integer, int[]>> out) throws Exception {
                    assert sequence.length == 0 || (sequence[0] == 0 && sequence[sequence.length - 1] == 0);
                    sort(sequence);

                    Arrays.fill(flags, false);
                    for (int i = 0; i < sequence.length; i++) {
                        if (sequence[i] == 0) {
                            continue;
                        }
                        int partition = sequence[i] % numPartitions;
                        if (!flags[partition]) {
                            flags[partition] = true;
                            int[] sub = Arrays.copyOfRange(sequence, i - 1, sequence.length);
                            sub[0] = 0;
                            out.collect(Tuple2.of(partition, sub));
                        }
                    }
                }

                /**
                 * Sort items within each elements.
                 */
                private void sort(int[] sequence) {
                    for (int start = 0; start < sequence.length; ) {
                        if (sequence[start] == 0) {
                            start++;
                            continue;
                        }
                        int end = start;
                        while (sequence[end] != 0) {
                            end++;
                        }
                        // sort in range [start, end)
                        Arrays.sort(sequence, start, end);
                        start = end + 1;
                    }
                }
            })
            .withBroadcastSet(itemCounts, "itemCounts");
    }

    /**
     * Locally generate all frequent patterns that starts with "prefixItem".
     *
     * @param allSequences     All local sequences.
     * @param initialPostfixes The initial postfixes.
     * @param prefixItem       The prefix item.
     * @param minSuppCnt       Minimum support.
     * @param maxPatternLength Maximum pattern length.
     * @param out              The output collector.
     */
    private static void generateFreqPattern(final List<int[]> allSequences,
                                            final List<Postfix> initialPostfixes,
                                            int prefixItem, long minSuppCnt, int maxPatternLength,
                                            Collector<Tuple2<int[], Integer>> out) {
        Stack<Node> stack = new Stack<>();
        Node root = new Node(new Prefix(new int[]{0, prefixItem, 0}, 1),
            Postfix.projectAll(initialPostfixes, allSequences, prefixItem));
        root.nextPrefixItems = Postfix.getAllNextPrefixItems(allSequences, root.projectedDB, minSuppCnt);
        root.emitResult(out);
        stack.push(root);

        // We use depth-first search to generate all frequent patterns
        while (!stack.empty()) {
            Node top = stack.peek();
            if (top.hasFinished()) {
                stack.pop();
            } else {
                int which = top.numFinished;
                int nextItem = top.nextPrefixItems[which];
                Prefix nextPrefix = top.prefix.expand(nextItem);
                List<Postfix> projectedDB = Postfix.projectAll(top.projectedDB, allSequences, nextItem);
                Node child = new Node(nextPrefix, projectedDB);
                child.emitResult(out); // pre-order traversal
                if (nextPrefix.length < maxPatternLength) {
                    child.nextPrefixItems = Postfix.getAllNextPrefixItems(allSequences, projectedDB, minSuppCnt);
                    stack.push(child);
                }
                top.numFinished++;
            }
        }
    }


    /**
     * A node in the frequent pattern tree.
     * A node has a prefix, a projected db, and a list of items to project to, which forms
     * its children nodes after projecting to each of those items.
     */
    private static class Node {
        Prefix prefix;
        List<Postfix> projectedDB;
        Integer[] nextPrefixItems;
        int numFinished;

        public Node(Prefix prefix, List<Postfix> projectedDB) {
            this.prefix = prefix;
            this.projectedDB = projectedDB;
            numFinished = 0;
        }

        boolean hasFinished() {
            return numFinished >= nextPrefixItems.length;
        }

        void emitResult(Collector<Tuple2<int[], Integer>> out) {
            out.collect(Tuple2.of(prefix.items, projectedDB.size()));
        }

        @Override
        public String toString() {
            StringBuilder sbd = new StringBuilder();
            sbd.append("Node[prefix=").append(prefix.toString());
            projectedDB.forEach(postfix -> {
                sbd.append(",postfix=").append(postfix.toString());
            });
            sbd.append(",nextPrefixItems=").append(Tuple1.of(nextPrefixItems));
            sbd.append(",numFinished=").append(numFinished).append("/").append(nextPrefixItems.length);
            sbd.append("]");
            return sbd.toString();
        }
    }

    /**
     * Prefix of a sequence. Refer to paper
     * "PrefixSpan: Mining Sequential Patterns Efficiently by Prefix-Projected" for the
     * accurate definition.
     */
    private static class Prefix {
        /**
         * Items in the prefix.
         */
        int[] items;

        /**
         * Length of the prefix.
         */
        int length;

        Prefix(int[] items, int length) {
            this.items = items;
            this.length = length;
        }

        /**
         * Expand the prefix by adding one item.
         *
         * @param item The item to append. A positive item indicates that it can be appended to the prefix,
         *             while a negative item indicates that it can be assembled to the last item set of the prefix.
         * @return The prefix after expansion.
         */

        public Prefix expand(int item) {
            if (item < 0) {
                int[] expanded = new int[items.length + 1];
                System.arraycopy(items, 0, expanded, 0, items.length - 1);
                expanded[items.length - 1] = -item;
                expanded[items.length] = 0;
                return new Prefix(expanded, length + 1);
            } else {
                int[] expanded = new int[items.length + 2];
                System.arraycopy(items, 0, expanded, 0, items.length);
                expanded[items.length] = item;
                expanded[items.length + 1] = 0;
                return new Prefix(expanded, length + 1);
            }
        }

        @Override
        public String toString() {
            return Tuple1.of(items).toString();
        }
    }

    /**
     * Postfix of a sequence. Refer to paper
     * "PrefixSpan: Mining Sequential Patterns Efficiently by Prefix-Projected" for the
     * accurate definition.
     * <p>
     * Because we are doing "pseudo" projection, we don't actually generate new sequences.
     * Instead, we use "sequence id, start, partial starts" tuple to represent a postfix.
     */
    private static class Postfix {
        /**
         * The id of the sequence in the original sequence database.
         */
        int sequenceId;

        /**
         * Full or partial start position of the postfix in the sequence.
         */
        int start;

        /**
         * The partial starts.
         */
        Integer[] partialStarts;

        Postfix(int sequenceId) {
            this.sequenceId = sequenceId;
            this.start = 0;
            this.partialStarts = new Integer[0];
        }

        Postfix(int sequenceId, int start, Integer[] partialStarts) {
            this.sequenceId = sequenceId;
            this.start = start;
            this.partialStarts = partialStarts;
        }

        /**
         * Project the postfixes against "item". In the projected db, we can mine all sequences
         * that starts with "item".
         *
         * @param initialPostfixes The postfixes to project.
         * @param allSequences     Original sequences, which are kept untouched all along since we are doing pseudo-projection.
         * @param item             The item against which the postfixes are projecting.
         * @return The postfixes after projection.
         */
        static List<Postfix> projectAll(final List<Postfix> initialPostfixes, final List<int[]> allSequences, int item) {
            List<Postfix> projectDB = new ArrayList<>();
            initialPostfixes.forEach(postfix -> {
                Postfix projected = postfix.project(allSequences, item);
                if (projected != null) {
                    projectDB.add(projected);
                }
            });
            return projectDB;
        }

        /**
         * Find all projectable items in the projected db for further projection.
         * A projectable item combined with its prefix should have counts >= "minSuppCnt".
         */
        static Integer[] getAllNextPrefixItems(
            final List<int[]> allSequences, final List<Postfix> projectedDB, long minSuppCnt) {
            Map<Integer, Integer> counts = new HashMap<>(); // prefix -> count
            projectedDB.forEach(postfix -> {
                Integer[] prefixItems = postfix.genPrefixItems(allSequences);
                for (Integer prefixItem : prefixItems) {
                    counts.merge(prefixItem, 1, (a, b) -> a + b);
                }
            });

            List<Integer> qualified = new ArrayList<>();
            counts.forEach((prefix, cnt) -> {
                if (cnt >= minSuppCnt) {
                    qualified.add(prefix);
                }
            });
            return qualified.toArray(new Integer[0]);
        }

        /**
         * Project the postfix with respect to input prefix item. "allSequences" are passed in
         * because we are doing so called "pseudo projection", where no actual projection happens
         * but just the positions in the sequence are recorded.
         */
        public Postfix project(final List<int[]> allSequences, int prefix) {
            int[] sequence = allSequences.get(this.sequenceId);
            int n1 = sequence.length - 1;
            boolean matched = false;
            int newStart = n1;
            List<Integer> newPartialStarts = new ArrayList<>();

            if (prefix > 0) {
                int fullStart = this.start;
                while (sequence[fullStart] != 0) {
                    fullStart++;
                }
                for (int i = fullStart; i < n1; i++) {
                    int x = sequence[i];
                    if (x == prefix) {
                        if (!matched) {
                            newStart = i;
                            matched = true;
                        }
                        if (sequence[i + 1] != 0) {
                            newPartialStarts.add(i + 1);
                        }
                    }
                }
            } else if (prefix < 0) {
                int target = -prefix;
                for (int start : this.partialStarts) {
                    int i = start;
                    int x = sequence[i];
                    while (x != target && x != 0) {
                        i += 1;
                        x = sequence[i];
                    }
                    if (x == target) {
                        i += 1;
                        if (!matched) {
                            newStart = i;
                            matched = true;
                        }
                        if (sequence[i] != 0) {
                            newPartialStarts.add(i);
                        }
                    }
                }
            }
            if (matched) {
                return new Postfix(this.sequenceId, newStart, newPartialStarts.toArray(new Integer[0]));
            } else {
                return null;
            }
        }

        /**
         * Get all candidate prefix items. A positive item indicates that it can be appended to the prefix,
         * while a negative item indicates that it can be assembled to the last itemset of the prefix.
         */
        Integer[] genPrefixItems(final List<int[]> allSequences) {
            int[] sequence = allSequences.get(this.sequenceId);
            long n1 = sequence.length - 1;
            Set<Integer> prefixes = new HashSet<>();

            // a) items that can be assembled to the last itemset of the prefix
            for (Integer partialStart : partialStarts) {
                int i = partialStart;
                int x = -sequence[i];
                while (x != 0) {
                    if (!prefixes.contains(x)) {
                        prefixes.add(x);
                    }
                    i += 1;
                    x = -sequence[i];
                }
            }

            // b) items that can be appended to the prefix
            int fullStart = this.start;
            while (sequence[fullStart] != 0) {
                fullStart++;
            }
            for (int i = fullStart; i < n1; i++) {
                int x = sequence[i];
                if (x != 0 && !prefixes.contains(x)) {
                    prefixes.add(x);
                }
            }

            return prefixes.toArray(new Integer[0]);
        }

        @Override
        public String toString() {
            return Tuple3.of(sequenceId, start, partialStarts).toString();
        }
    }
}