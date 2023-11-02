package com.alibaba.alink.operator.batch.graph;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MultiSourceShortestPathBatchOpTest extends AlinkTestBase {

    @Test
    public void test() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0, 1, 0.4),
            Row.of(1, 2, 1.3),
            Row.of(2, 3, 1.0),
            Row.of(3, 4, 1.0),
            Row.of(4, 5, 1.0),
            Row.of(5, 6, 1.0),
            Row.of(6, 7, 1.0),
            Row.of(7, 8, 1.0),
            Row.of(8, 9, 1.0),
            Row.of(9, 6, 2.0),
            Row.of(19, 16, 1.0),
        };
        Row[] sources = new Row[]{
            Row.of(1, 1),
            Row.of(5, 5),
        };
        BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
        BatchOperator sourceBatchOp = new MemSourceBatchOp(sources, new String[]{"source", "target"});
        MultiSourceShortestPathBatchOp op = new MultiSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setEdgeWeightCol("weight")
            .setSourcePointCol("source");

        BatchOperator res = op.linkFrom(inData, sourceBatchOp);
        res.lazyPrint(20);
        List<Row> rowlist = res.collect();
        List <Tuple4 <Integer, Integer, String, Double>> result = new ArrayList <>();
        for (Row row : rowlist) {
            result.add(Tuple4.of(
                (Integer)row.getField(0),
                (Integer) row.getField(1),
                (String) row.getField(2),
                (Double) row.getField(3)
            ));
        }

        List<Tuple4 <Integer, Integer, String, Double>> expectedResultList = new ArrayList <>();
        expectedResultList.add(Tuple4.of(19, null, "", -1.0D));
        expectedResultList.add(Tuple4.of(16, null, "", -1.0D));
        expectedResultList.add(Tuple4.of(0, 1, "0,1", 0.4D));
        expectedResultList.add(Tuple4.of(1, 1, "1", 0.0D));
        expectedResultList.add(Tuple4.of(2, 1, "2,1", 1.3D));
        expectedResultList.add(Tuple4.of(3, 5, "3,4,5", 2.0D));
        expectedResultList.add(Tuple4.of(4, 5, "4,5", 1.0D));
        expectedResultList.add(Tuple4.of(5, 5, "5", 0.0D));
        expectedResultList.add(Tuple4.of(7, 5, "7,6,5", 2.0D));
        expectedResultList.add(Tuple4.of(6, 5, "6,5", 1.0D));
        expectedResultList.add(Tuple4.of(8, 5, "8,7,6,5", 3.0D));
        expectedResultList.add(Tuple4.of(9, 5, "9,6,5", 3.0D));

        compareResultCollections(
            result,
            expectedResultList,
            new TupleComparator<Tuple4 <Integer, Integer, String, Double>>()
        );
    }

    @Test
    public void testMaxIter() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0, 1, 0.4),
            Row.of(1, 2, 1.3),
            Row.of(2, 3, 1.0),
            Row.of(3, 4, 1.0),
            Row.of(4, 5, 1.0),
            Row.of(5, 6, 1.0),
            Row.of(6, 7, 1.0),
            Row.of(7, 8, 1.0),
            Row.of(8, 9, 1.0),
            Row.of(9, 6, 2.0),
            Row.of(19, 16, 1.0),
        };
        Row[] sources = new Row[]{
            Row.of(1, 1),
            Row.of(5, 5),
        };
        BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
        BatchOperator sourceBatchOp = new MemSourceBatchOp(sources, new String[]{"source", "target"});
        MultiSourceShortestPathBatchOp op = new MultiSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setEdgeWeightCol("weight")
            .setMaxIter(2)
            .setSourcePointCol("source");

        BatchOperator res = op.linkFrom(inData, sourceBatchOp);
        res.lazyPrint(20);
        List<Row> rowlist = res.collect();
        List <Tuple4 <Integer, Integer, String, Double>> result = new ArrayList <>();
        for (Row row : rowlist) {
            result.add(Tuple4.of(
                (Integer)row.getField(0),
                (Integer) row.getField(1),
                (String) row.getField(2),
                (Double) row.getField(3)
            ));
        }

        List<Tuple4 <Integer, Integer, String, Double>> expectedResultList = new ArrayList <>();
        expectedResultList.add(Tuple4.of(19, null, "", -1.0D));
        expectedResultList.add(Tuple4.of(16, null, "", -1.0D));
        expectedResultList.add(Tuple4.of(0, 1, "0,1", 0.4D));
        expectedResultList.add(Tuple4.of(1, 1, "1", 0.0D));
        expectedResultList.add(Tuple4.of(2, 1, "2,1", 1.3D));
        expectedResultList.add(Tuple4.of(3, 5, "3,4,5", 2.0D));
        expectedResultList.add(Tuple4.of(4, 5, "4,5", 1.0D));
        expectedResultList.add(Tuple4.of(5, 5, "5", 0.0D));
        expectedResultList.add(Tuple4.of(7, 5, "7,6,5", 2.0D));
        expectedResultList.add(Tuple4.of(6, 5, "6,5", 1.0D));
        expectedResultList.add(Tuple4.of(8, null, "", -1.0D));
        expectedResultList.add(Tuple4.of(9, 5, "9,6,5", 3.0D));

        compareResultCollections(
            result,
            expectedResultList,
            new TupleComparator<Tuple4 <Integer, Integer, String, Double>>()
        );
    }

    @Test
    public void testDirected() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0, 1, 0.4),
            Row.of(1, 2, 1.3),
            Row.of(2, 3, 1.0),
            Row.of(3, 4, 1.0),
            Row.of(4, 5, 1.0),
            Row.of(5, 6, 1.0),
            Row.of(6, 7, 1.0),
            Row.of(7, 8, 1.0),
            Row.of(8, 9, 1.0),
            Row.of(9, 6, 2.0),
            Row.of(19, 16, 1.0),
        };
        Row[] sources = new Row[]{
            Row.of(1, 1),
            Row.of(5, 5),
        };
        BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
        BatchOperator sourceBatchOp = new MemSourceBatchOp(sources, new String[]{"source", "target"});
        MultiSourceShortestPathBatchOp op = new MultiSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setEdgeWeightCol("weight")
            .setAsUndirectedGraph(false)
            .setSourcePointCol("source");

        BatchOperator res = op.linkFrom(inData, sourceBatchOp);
        res.lazyPrint(20);
        List<Row> rowlist = res.collect();
        List <Tuple4 <Integer, Integer, String, Double>> result = new ArrayList <>();
        for (Row row : rowlist) {
            result.add(Tuple4.of(
                (Integer)row.getField(0),
                (Integer) row.getField(1),
                (String) row.getField(2),
                (Double) row.getField(3)
            ));
        }

        List<Tuple4 <Integer, Integer, String, Double>> expectedResultList = new ArrayList <>();
        expectedResultList.add(Tuple4.of(19, null, "", -1.0D));
        expectedResultList.add(Tuple4.of(16, null, "", -1.0D));
        expectedResultList.add(Tuple4.of(0, null, "", -1.0D));
        expectedResultList.add(Tuple4.of(1, 1, "1", 0.0D));
        expectedResultList.add(Tuple4.of(2, 1, "2,1", 1.3D));
        expectedResultList.add(Tuple4.of(3, 1, "3,2,1", 2.3D));
        expectedResultList.add(Tuple4.of(4, 1, "4,3,2,1", 3.3D));
        expectedResultList.add(Tuple4.of(5, 5, "5", 0.0D));
        expectedResultList.add(Tuple4.of(7, 5, "7,6,5", 2.0D));
        expectedResultList.add(Tuple4.of(6, 5, "6,5", 1.0D));
        expectedResultList.add(Tuple4.of(8, 5, "8,7,6,5", 3.0D));
        expectedResultList.add(Tuple4.of(9, 5, "9,8,7,6,5", 4.0D));

        compareResultCollections(
            result,
            expectedResultList,
            new TupleComparator<Tuple4 <Integer, Integer, String, Double>>()
        );
    }
}