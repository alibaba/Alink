package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SingleSourceShortestPathBatchOpTest extends AlinkTestBase {

    Row[] rows = new Row[]{
        Row.of(1, 2, 3.3),
        Row.of(2, 3, 1.0),
        Row.of(3, 4, 1.0),
        Row.of(4, 5, 1.0),
        Row.of(5, 6, 1.0),
        Row.of(6, 7, 1.0),
        Row.of(7, 8, 1.0),
        Row.of(8, 9, 1.0),
        Row.of(9, 6, 1.0),
        Row.of(19, 16, 1.0),
    };

    @Test
    public void test() throws Exception {
        BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
        SingleSourceShortestPathBatchOp op = new SingleSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setEdgeWeightCol("weight")
            .setSourcePoint("1");

        BatchOperator res = op.linkFrom(inData);

        List<Row> list = res.collect();
        List < Tuple2 <Integer, Double>> result = new ArrayList <>();
        for (Row row : list) {
            result.add(Tuple2.of((Integer)row.getField(0), (Double)row.getField(1)));
        }

        List<Tuple2 <Integer, Double>> expectedResultList = new ArrayList <>();
        expectedResultList.add(Tuple2.of(1, 0.0D));
        expectedResultList.add(Tuple2.of(19, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(3, 4.3D));
        expectedResultList.add(Tuple2.of(8, 9.3D));
        expectedResultList.add(Tuple2.of(7, 8.3D));
        expectedResultList.add(Tuple2.of(16, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(2, 3.3D));
        expectedResultList.add(Tuple2.of(4, 5.3D));
        expectedResultList.add(Tuple2.of(5, 6.3D));
        expectedResultList.add(Tuple2.of(9, 8.3D));
        expectedResultList.add(Tuple2.of(6, 7.3D));

        compareResultCollections(
            result,
            expectedResultList,
            new TupleComparator<Tuple2 <Integer, Double>>()
        );
    }

    @Test
    public void testMaxIter() throws Exception {

        BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
        SingleSourceShortestPathBatchOp op = new SingleSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setEdgeWeightCol("weight")
            .setMaxIter(3)
            .setSourcePoint("1");

        BatchOperator res = op.linkFrom(inData);
        res.lazyPrint();
        List<Row> list = res.collect();
        List < Tuple2 <Integer, Double>> result = new ArrayList <>();
        for (Row row : list) {
            result.add(Tuple2.of((Integer)row.getField(0), (Double)row.getField(1)));
        }

        List<Tuple2 <Integer, Double>> expectedResultList = new ArrayList <>();
        expectedResultList.add(Tuple2.of(1, 0.0D));
        expectedResultList.add(Tuple2.of(19, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(3, 4.3D));
        expectedResultList.add(Tuple2.of(8, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(7, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(16, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(2, 3.3D));
        expectedResultList.add(Tuple2.of(4, 5.3D));
        expectedResultList.add(Tuple2.of(5, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(9, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(6, Double.POSITIVE_INFINITY));

        compareResultCollections(
            result,
            expectedResultList,
            new TupleComparator<Tuple2 <Integer, Double>>()
        );
    }

    @Test
    public void testLongInput() throws Exception {
        Row[] longRows = new Row[]{
            Row.of(1L, 2L, 3.3),
            Row.of(2L, 3L, 1.0),
            Row.of(3L, 4L, 1.0),
            Row.of(4L, 5L, 1.0),
            Row.of(5L, 6L, 1.0),
            Row.of(6L, 7L, 1.0),
            Row.of(7L, 8L, 1.0),
            Row.of(8L, 9L, 1.0),
            Row.of(9L, 6L, 1.0),
            Row.of(19L, 16L, 1.0),
        };

        BatchOperator inData = new MemSourceBatchOp(longRows, new String[]{"source", "target", "weight"});
        SingleSourceShortestPathBatchOp op = new SingleSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setEdgeWeightCol("weight")
            .setAsUndirectedGraph(false)
            .setSourcePoint("1");

        BatchOperator res = op.linkFrom(inData);
        res.lazyPrint();
        List<Row> list = res.collect();
        List < Tuple2 <Long, Double>> result = new ArrayList <>();
        for (Row row : list) {
            result.add(Tuple2.of((Long)row.getField(0), (Double)row.getField(1)));
        }

        List<Tuple2 <Long, Double>> expectedResultList = new ArrayList <>();
        expectedResultList.add(Tuple2.of(1L, 0.0D));
        expectedResultList.add(Tuple2.of(19L, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(3L, 4.3D));
        expectedResultList.add(Tuple2.of(8L, 9.3D));
        expectedResultList.add(Tuple2.of(7L, 8.3D));
        expectedResultList.add(Tuple2.of(16L, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(2L, 3.3D));
        expectedResultList.add(Tuple2.of(4L, 5.3D));
        expectedResultList.add(Tuple2.of(5L, 6.3D));
        expectedResultList.add(Tuple2.of(9L, 10.3D));
        expectedResultList.add(Tuple2.of(6L, 7.3D));

        compareResultCollections(
            result,
            expectedResultList,
            new TupleComparator<Tuple2 <Long, Double>>()
        );
    }

    @Test
    public void testDirected() throws Exception {

        BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
        SingleSourceShortestPathBatchOp op = new SingleSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setEdgeWeightCol("weight")
            .setAsUndirectedGraph(false)
            .setSourcePoint("1");

        BatchOperator res = op.linkFrom(inData);
        res.lazyPrint();
        List<Row> list = res.collect();
        List < Tuple2 <Integer, Double>> result = new ArrayList <>();
        for (Row row : list) {
            result.add(Tuple2.of((Integer)row.getField(0), (Double)row.getField(1)));
        }

        List<Tuple2 <Integer, Double>> expectedResultList = new ArrayList <>();
        expectedResultList.add(Tuple2.of(1, 0.0D));
        expectedResultList.add(Tuple2.of(19, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(3, 4.3D));
        expectedResultList.add(Tuple2.of(8, 9.3D));
        expectedResultList.add(Tuple2.of(7, 8.3D));
        expectedResultList.add(Tuple2.of(16, Double.POSITIVE_INFINITY));
        expectedResultList.add(Tuple2.of(2, 3.3D));
        expectedResultList.add(Tuple2.of(4, 5.3D));
        expectedResultList.add(Tuple2.of(5, 6.3D));
        expectedResultList.add(Tuple2.of(9, 10.3D));
        expectedResultList.add(Tuple2.of(6, 7.3D));

        compareResultCollections(
            result,
            expectedResultList,
            new TupleComparator<Tuple2 <Integer, Double>>()
        );
    }

}