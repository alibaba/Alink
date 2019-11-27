package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import com.alibaba.alink.operator.common.dataproc.SortUtils;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * It will calculation rank of multiple columns with ascending order.
 * It will used to calculate spearman correlation later.
 */
public class SpearmanCorrelation {

    /**
     * @param outputIsVector: output format is vector or not.
     * @return rank by col.
     */
    public static DataSet<Row> calcRank(DataSet<Row> data, Boolean outputIsVector) {
        // append_id, last value is id.
        DataSet<Row> inputWithId = data.map(new AppendIdBatchOp.AppendIdMapper());

        //rowNum of dataset
        DataSet<Long> cnt = DataSetUtils.countElementsPerPartition(inputWithId)
            .sum(1)
            .map(new MapFunction<Tuple2<Integer, Long>, Long>() {
                @Override
                public Long map(Tuple2<Integer, Long> value) {
                    return value.f1;
                }
            });

        //flatten data to triple, f0 is colId, f1 is rowId, f2 is value;
        DataSet<Row> flatten = inputWithId.flatMap(
            new FlatMapFunction<Row, Row>() {
                @Override
                public void flatMap(Row value, Collector<Row> out) {
                    long rowId = (long) value.getField(value.getArity() - 1);
                    for (int i = 0; i < value.getArity() - 1; ++i) {
                        out.collect(Row.of(new TripleComparable<>(i, rowId, (Number) value.getField(i))));
                    }
                }
            });

        // sort value  group by colId
        Tuple2<DataSet<Tuple2<Integer, Row>>, DataSet<Tuple2<Integer, Long>>> sortedData
            = SortUtils.pSort(flatten, 0);


        //compute rank
        return sortedData.f0
            .groupBy(0)
            .withPartitioner(new SortUtils.AvgPartition())
            .reduceGroup(new MultiRank())
            .withBroadcastSet(sortedData.f1, "counts")
            .withBroadcastSet(cnt, "totalCnt")
            .groupBy(1)
            .withPartitioner(new SortUtils.AvgLongPartitioner())
            .reduceGroup(new CombineRank(outputIsVector));
    }

    /**
     * TripleComparable which compare with first and third value.
     */
    public static class TripleComparable<T0 extends Comparable, T1 extends Number, T2 extends Number>
        implements Comparable<TripleComparable> {
        static final SortUtils.ComparableComparator OBJECT_COMPARATOR = new SortUtils.ComparableComparator();
        public T0 first;
        public T1 second;
        public T2 third;

        TripleComparable(T0 first, T1 second, T2 third) {
            this.first = first;
            this.second = second;
            this.third = third;
        }

        @Override
        public int compareTo(TripleComparable o) {
            int f = this.first.compareTo(o.first);

            return f == 0 ? OBJECT_COMPARATOR.compare(this.third, o.third) : f;
        }
    }

    /**
     * combine rank, convert triple(colId, rowId, rank) to matrix.
     */
    public static class CombineRank extends RichGroupReduceFunction<Tuple3<Integer, Long, Long>, Row> {
        private Boolean outputIsVector;

        CombineRank(boolean outputIsVector) {
            this.outputIsVector = outputIsVector;
        }

        @Override
        public void reduce(Iterable<Tuple3<Integer, Long, Long>> iterable, Collector<Row> collector)
            throws Exception {
            int colNum = -1;

            List<Tuple3<Integer, Long, Long>> tuple3List = new ArrayList<>();
            for (Tuple3<Integer, Long, Long> tuple3 : iterable) {
                if (colNum < tuple3.f0) {
                    colNum = tuple3.f0;
                }
                tuple3List.add(tuple3);
            }
            colNum += 1;

            double[] data = new double[colNum];
            for (Tuple3<Integer, Long, Long> tuple3 : tuple3List) {
                data[tuple3.f0] = (double) tuple3.f2;
            }

            if (outputIsVector) {
                Row out = new Row(1);

                out.setField(0, VectorUtil.toString(new DenseVector(data)));

                collector.collect(out);

            } else {
                Row out = new Row(colNum);

                for (int i = 0; i < colNum; i++) {
                    out.setField(i, data[i]);
                }

                collector.collect(out);
            }
        }
    }

    /**
     * compute rank.
     * return tuple3: f0 is colId, f1 is rowId, f2 is rank.
     */
    public static class MultiRank
        extends RichGroupReduceFunction<Tuple2<Integer, Row>, Tuple3<Integer, Long, Long>> {
        private List<Tuple2<Integer, Long>> counts;
        private long totalCnt = 0;

        MultiRank() {

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.counts = getRuntimeContext().getBroadcastVariableWithInitializer(
                "counts",
                new BroadcastVariableInitializer<Tuple2<Integer, Long>, List<Tuple2<Integer, Long>>>() {
                    @Override
                    public List<Tuple2<Integer, Long>> initializeBroadcastVariable(Iterable<Tuple2<Integer, Long>> data) {
                        List<Tuple2<Integer, Long>> sortedData = new ArrayList<>();
                        for (Tuple2<Integer, Long> datum : data) {
                            sortedData.add(datum);
                        }
                        sortedData.sort(Comparator.comparing(o -> o.f0));

                        return sortedData;

                    }
                }
            );


            this.totalCnt = getRuntimeContext().getBroadcastVariableWithInitializer("totalCnt",
                new BroadcastVariableInitializer<Long, Long>() {
                    @Override
                    public Long initializeBroadcastVariable(Iterable<Long> data) {
                        return data.iterator().next();
                    }
                });
        }

        private int findCurCount(int id) {
            for (Tuple2<Integer, Long> count : counts) {
                int curId = count.f0;
                if (curId == id) {
                    return count.f1.intValue();
                }
            }
            throw new RuntimeException("Error key. key: " + id);
        }

        @Override
        public void reduce(Iterable<Tuple2<Integer, Row>> values, Collector<Tuple3<Integer, Long, Long>> out) {
            Row[] allRows = null;
            int id = -1;

            int allRowsId = 0;
            for (Tuple2<Integer, Row> value : values) {
                id = value.f0;
                if (allRows == null) {
                    allRows = new Row[findCurCount(id)];
                }
                allRows[allRowsId] = value.f1;
                allRowsId++;
            }

            if (allRows == null) {
                return;
            }

            SortUtils.RowComparator rowComparator = new SortUtils.RowComparator(0);
            SortUtils.ComparableComparator objCompare = new SortUtils.ComparableComparator();

            Arrays.sort(allRows, rowComparator);

            long start = 0;
            for (Tuple2<Integer, Long> count : counts) {
                int curId = count.f0;

                if (curId == id) {
                    break;
                }

                if (curId > id) {
                    throw new RuntimeException("Error curId: " + curId
                        + ". id: " + id);
                }

                start += count.f1;
            }

            long pos = start;
            long rank = 0;

            Number oldValue = null;
            Integer oldCol = null;
            Number newValue;

            for (Row row : allRows) {
                Tuple3<Integer, Long, Long> outTuple = new Tuple3<>();
                TripleComparable triple = (TripleComparable) row.getField(0);
                outTuple.f0 = (Integer) triple.first;
                outTuple.f1 = (Long) triple.second;

                if (oldCol == null) {
                    oldCol = outTuple.f0;
                    rank = pos % totalCnt + 1;
                    oldValue = triple.third;
                } else {
                    if (!oldCol.equals(outTuple.f0)) {
                        rank = 1;
                        oldValue = triple.third;
                        oldCol = outTuple.f0;
                    } else {
                        newValue = triple.third;
                        if (1 != objCompare.compare(oldValue, newValue)) {
                            oldValue = newValue;
                            rank++;
                        }
                    }
                }

                outTuple.f2 = rank;
                out.collect(outTuple);
                pos++;
            }
        }
    }
}
