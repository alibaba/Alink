package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.SplitParams;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.*;

/**
 * Split a dataset into two parts.
 */
public final class SplitBatchOp extends BatchOperator<SplitBatchOp>
    implements SplitParams<SplitBatchOp> {

    public SplitBatchOp() {
        this(new Params());
    }

    public SplitBatchOp(Params params) {
        super(params);
    }

    public SplitBatchOp(double fraction) {
        this(new Params().set(FRACTION, fraction));
    }

    @Override
    public SplitBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        final double fraction = getFraction();
        if (fraction < 0. || fraction > 1.0) {
            throw new RuntimeException("invalid fraction " + fraction);
        }

        DataSet<Row> rows = in.getDataSet();

        DataSet<Tuple2<Integer, Long>> countsPerPartition = DataSetUtils.countElementsPerPartition(rows);
        DataSet<long[]> numPickedPerPartition = countsPerPartition
            .mapPartition(new CountInPartition(fraction))
            .setParallelism(1)
            .name("decide_count_of_each_partition");

        DataSet<Row> out = rows
            .mapPartition(new PickInPartition())
            .withBroadcastSet(numPickedPerPartition, "counts")
            .name("pick_in_each_partition");

        this.setOutput(out, in.getSchema());
        this.setSideOutputTables(new Table[]{in.getOutputTable().minusAll(this.getOutputTable())});
        return this;
    }

    /**
     * Randomly decide the number of elements to select in each task
     */
    private static class CountInPartition extends RichMapPartitionFunction<Tuple2<Integer, Long>, long[]> {
        private double fraction;

        public CountInPartition(double fraction) {
            this.fraction = fraction;
        }

        @Override
        public void mapPartition(Iterable<Tuple2<Integer, Long>> values, Collector<long[]> out) throws Exception {
            Preconditions.checkArgument(getRuntimeContext().getIndexOfThisSubtask() == 0);

            long totCount = 0L;
            List<Tuple2<Integer, Long>> buffer = new ArrayList<>();
            for (Tuple2<Integer, Long> value : values) {
                totCount += value.f1;
                buffer.add(value);
            }

            int npart = buffer.size(); // num tasks
            long[] eachCount = new long[npart];
            long numTarget = Math.round((totCount * fraction));
            long[] eachSelect = new long[npart];

            for (Tuple2<Integer, Long> value : buffer) {
                eachCount[value.f0] = value.f1;
            }

            long totSelect = 0L;
            for (int i = 0; i < npart; i++) {
                eachSelect[i] = Math.round(Math.floor(eachCount[i] * fraction));
                totSelect += eachSelect[i];
            }

            if (totSelect < numTarget) {
                long remain = numTarget - totSelect;
                remain = Math.min(remain, totCount - totSelect);
                if (remain == totCount - totSelect) {
                    for (int i = 0; i < npart; i++) {
                        eachSelect[i] = eachCount[i];
                    }
                } else {
                    // select 'remain' out of 'npart'
                    List<Integer> shuffle = new ArrayList<>(npart);
                    while (remain > 0) {
                        for (int i = 0; i < npart; i++) {
                            shuffle.add(i);
                        }
                        Collections.shuffle(shuffle, new Random());
                        for (int i = 0; i < Math.min(remain, npart); i++) {
                            int taskId = shuffle.get(i);
                            while (eachSelect[taskId] >= eachCount[taskId]) {
                                taskId = (taskId + 1) % npart;
                            }
                            eachSelect[taskId]++;
                        }
                        remain -= npart;
                    }
                }
            }

            long[] statistics = new long[npart * 2];
            for (int i = 0; i < npart; i++) {
                statistics[i] = eachCount[i];
                statistics[i + npart] = eachSelect[i];
            }
            out.collect(statistics);

        }
    }

    /**
     * Randomly pick elements in each task
     */
    private static class PickInPartition extends RichMapPartitionFunction<Row, Row> {
        @Override
        public void mapPartition(Iterable<Row> values, Collector<Row> out)
            throws Exception {

            int npart = getRuntimeContext().getNumberOfParallelSubtasks();
            List<long[]> bc = getRuntimeContext().getBroadcastVariable("counts");
            long[] eachCount = Arrays.copyOfRange(bc.get(0), 0, npart);
            long[] eachSelect = Arrays.copyOfRange(bc.get(0), npart, npart * 2);

            if (bc.get(0).length / 2 != getRuntimeContext().getNumberOfParallelSubtasks()) {
                throw new RuntimeException("parallelism has changed");
            }

            int taskId = getRuntimeContext().getIndexOfThisSubtask();

            // emit the selected
            int[] selected = null;
            int iRow = 0;
            int numEmits = 0;
            for (Row row : values) {
                if (0 == iRow) {
                    long count = eachCount[taskId];
                    long select = eachSelect[taskId];

                    List<Integer> shuffle = new ArrayList<>((int) count);
                    for (int i = 0; i < count; i++) {
                        shuffle.add(i);
                    }
                    Collections.shuffle(shuffle, new Random(taskId));

                    selected = new int[(int) select];
                    for (int i = 0; i < select; i++) {
                        selected[i] = shuffle.get(i);
                    }
                    Arrays.sort(selected);
                }

                if (numEmits < selected.length && iRow == selected[numEmits]) {
                    out.collect(row);
                    numEmits++;
                }
                iRow++;
            }
        }
    }
}
