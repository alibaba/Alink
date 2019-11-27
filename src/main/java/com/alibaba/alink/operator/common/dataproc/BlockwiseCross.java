package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class BlockwiseCross implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(BlockwiseCross.class);

    public interface ScoreFunction<T1, T2> extends Function, Serializable {
        float score(Long id1, T1 v1, Long id2, T2 v2);
    }

    public interface BulkScoreFunction<T1, T2> extends Function, Serializable {
        void addTargets(Iterable<Tuple3<Integer, Long, T2>> iterable);

        List<Tuple2<Long, Float>> scoreAll(Long id1, T1 v1);
    }

    private static class DefaultBulkScoreFunction<T1, T2> implements BulkScoreFunction<T1, T2> {
        transient private List<Tuple2<Long, T2>> targets;
        transient private List<Tuple2<Long, Float>> scoreBuffer;
        private ScoreFunction<T1, T2> scoreFunction;

        DefaultBulkScoreFunction(ScoreFunction<T1, T2> scoreFunction) {
            this.scoreFunction = scoreFunction;
        }

        @Override
        public void addTargets(Iterable<Tuple3<Integer, Long, T2>> iterable) {
            targets = new ArrayList<>();
            scoreBuffer = new ArrayList<>();

            iterable.forEach(target -> {
                this.targets.add(Tuple2.of(target.f1, target.f2));
                this.scoreBuffer.add(Tuple2.of(target.f1, 0.F));
            });
        }

        @Override
        public List<Tuple2<Long, Float>> scoreAll(Long id1, T1 v1) {
            for (int i = 0; i < targets.size(); i++) {
                float score = scoreFunction.score(id1, v1, targets.get(i).f0, targets.get(i).f1);
                scoreBuffer.get(i).setFields(targets.get(i).f0, score);
            }
            return scoreBuffer;
        }
    }

    public static <T1, T2> DataSet<Tuple3<Long, long[], float[]>>
    findTopK(DataSet<Tuple2<Long, T1>> dataSet1, DataSet<Tuple2<Long, T2>> dataSet2,
             final int k, final Order order,
             final ScoreFunction<T1, T2> scoreFunction) {
        return findTopK(dataSet1, dataSet2, k, order, new DefaultBulkScoreFunction<>(scoreFunction));
    }

    @SuppressWarnings("unchecked")
    public static <T1, T2> DataSet<Tuple3<Long, long[], float[]>>
    findTopK(DataSet<Tuple2<Long, T1>> dataSet1, DataSet<Tuple2<Long, T2>> dataSet2,
             final int k, final Order order,
             final BulkScoreFunction<T1, T2> bulkScoreFunction) {

        // Re-balance data to achieve better performance.
        dataSet1 = dataSet1.rebalance();
        dataSet2 = dataSet2.rebalance();

        final int parallelism = dataSet1.getExecutionEnvironment().getParallelism();
        DataSet<Tuple3<Integer, Long, T1>> dataSet1WithTaskId = appendTaskId(dataSet1);
        DataSet<Tuple3<Integer, Long, T2>> dataSet2WithTaskId = appendTaskId(dataSet2);

        TypeInformation type1 = ((TupleTypeInfo) dataSet1.getType()).getTypeAt(1);
        TypeInformation stateType = new TupleTypeInfo(Types.INT, Types.LONG, type1,
            new TypeHint<PriorityQueue<Tuple2<Long, Float>>>() {
            }.getTypeInfo());

        DataSet<Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>> topk =
            dataSet1WithTaskId
                .map(new RichMapFunction<Tuple3<Integer, Long, T1>, Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>>() {
                    @Override
                    public Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>> map(Tuple3<Integer, Long, T1> value) throws Exception {
                        PriorityQueue<Tuple2<Long, Float>> priorityQueue = new PriorityQueue<>(new Comparator<Tuple2<Long, Float>>() {
                            @Override
                            public int compare(Tuple2<Long, Float> o1, Tuple2<Long, Float> o2) {
                                if (order == Order.DESCENDING) {
                                    return Float.compare(o1.f1, o2.f1);
                                } else if (order == Order.ASCENDING) {
                                    return Float.compare(o2.f1, o1.f1);
                                } else {
                                    throw new IllegalArgumentException("Not supported order type: " + order);
                                }
                            }
                        });
                        return Tuple4.of(value.f0, value.f1, value.f2, priorityQueue);
                    }
                })
                .returns(stateType)
                .withForwardedFields("f0;f1;f2");

        IterativeDataSet<Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>> loop = topk.iterate(parallelism);

        DataSet<Integer> shift = loop
            .mapPartition(new RichMapPartitionFunction<Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>, Integer>() {
                @Override
                public void mapPartition(Iterable<Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>> values, Collector<Integer> out) throws Exception {
                    if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                        out.collect(getIterationRuntimeContext().getSuperstepNumber() - 1);
                    }
                }
            })
            .returns(Types.INT);

        DataSet<Tuple3<Integer, Long, T2>> shiftedDataSet2 = dataSet2WithTaskId
            .map(new RichMapFunction<Tuple3<Integer, Long, T2>, Tuple3<Integer, Long, T2>>() {
                transient private int shift;

                @Override
                public void open(Configuration parameters) throws Exception {
                    this.shift = (Integer) getRuntimeContext().getBroadcastVariable("shift").get(0);
                }

                @Override
                public Tuple3<Integer, Long, T2> map(Tuple3<Integer, Long, T2> value) throws Exception {
                    return Tuple3.of((value.f0 + shift) % parallelism, value.f1, value.f2);
                }
            })
            .withBroadcastSet(shift, "shift")
            .returns(dataSet2WithTaskId.getType())
            .withForwardedFields("f1;f2");

        topk = loop.coGroup(shiftedDataSet2)
            .where(0).equalTo(0)
            .withPartitioner(new Partitioner<Integer>() {
                @Override
                public int partition(Integer key, int numPartitions) {
                    return key % numPartitions;
                }
            })
            .with(new RichCoGroupFunction<Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>,
                Tuple3<Integer, Long, T2>,
                Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>>() {
                @Override
                public void coGroup(Iterable<Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>> records,
                                    Iterable<Tuple3<Integer, Long, T2>> targets,
                                    Collector<Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>> out) throws Exception {
                    if (records == null) {
                        return;
                    }

                    if (targets == null) {
                        records.forEach(out::collect);
                        return;
                    }

                    long t0 = System.currentTimeMillis();
                    bulkScoreFunction.addTargets(targets);

                    double tScore = 0.;
                    double tEnqueue = 0.;
                    int numRecords = 0;
                    int numTargets = 0;

                    for (Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>> record : records) {
                        long tt0 = System.currentTimeMillis();
                        numRecords++;
                        List<Tuple2<Long, Float>> scores = bulkScoreFunction.scoreAll(record.f1, record.f2);
                        numTargets = scores.size();

                        long tt1 = System.currentTimeMillis();
                        PriorityQueue<Tuple2<Long, Float>> priorQueue = record.f3;

                        for (int i = 0; i < scores.size(); i++) {
                            float score = scores.get(i).f1;
                            Long targetId = scores.get(i).f0;
                            if (priorQueue.size() < k) {
                                priorQueue.add(Tuple2.of(targetId, score));
                            } else {
                                boolean replace = (order == Order.DESCENDING && score > priorQueue.peek().f1 ||
                                    order == Order.ASCENDING && score < priorQueue.peek().f1);
                                if (replace) {
                                    priorQueue.poll();
                                    priorQueue.add(Tuple2.of(targetId, score));
                                }
                            }
                        }

                        long tt2 = System.currentTimeMillis();
                        tScore += 0.001 * (tt1 - tt0);
                        tEnqueue += 0.001 * (tt2 - tt1);

                        out.collect(record);
                    }

                    long t1 = System.currentTimeMillis();
                    LOG.info("Done local cross in {}s, # records {}, # targets {}", (t1 - t0) * 0.001, numRecords, numTargets);
                    LOG.info("Wall time: score {}s, enqueue {}s", tScore, tEnqueue);
                }
            })
            .returns(stateType)
            .name("block_cross");

        topk = loop.closeWith(topk);

        return topk
            .map(new MapFunction<Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>>, Tuple3<Long, long[], float[]>>() {
                @Override
                public Tuple3<Long, long[], float[]> map(Tuple4<Integer, Long, T1, PriorityQueue<Tuple2<Long, Float>>> value) throws Exception {
                    PriorityQueue<Tuple2<Long, Float>> priorQueue = value.f3;
                    long[] targets = new long[priorQueue.size()];
                    float[] scores = new float[priorQueue.size()];
                    int pos = priorQueue.size() - 1;
                    while (priorQueue.size() > 0) {
                        Tuple2<Long, Float> target = priorQueue.poll();
                        targets[pos] = target.f0;
                        scores[pos] = target.f1;
                        pos--;
                    }
                    return Tuple3.of(value.f1, targets, scores);
                }
            })
            .returns(new TypeHint<Tuple3<Long, long[], float[]>>() {
            });
    }

    private static <T> DataSet<Tuple3<Integer, Long, T>> appendTaskId(DataSet<Tuple2<Long, T>> dataset) {
        TypeInformation type = ((TupleTypeInfo) dataset.getType()).getTypeAt(1);

        return dataset
            .map(new RichMapFunction<Tuple2<Long, T>, Tuple3<Integer, Long, T>>() {
                transient private int taskId;

                @Override
                public void open(Configuration parameters) throws Exception {
                    this.taskId = getRuntimeContext().getIndexOfThisSubtask();
                }

                @Override
                public Tuple3<Integer, Long, T> map(Tuple2<Long, T> value) throws Exception {
                    return Tuple3.of(taskId, value.f0, value.f1);
                }
            })
            .returns(new TupleTypeInfo<>(Types.INT, Types.LONG, type))
            .withForwardedFields("f0->f1;f1->f2");
    }
}
