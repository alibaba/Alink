package com.alibaba.alink.operator.batch.graph;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SingleSourceShortestPathBatchOpTest extends AlinkTestBase {

    @Test
    public void test() throws Exception {
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
        BatchOperator inData = new MemSourceBatchOp(rows, new String[]{"source", "target", "weight"});
        SingleSourceShortestPathBatchOp op = new SingleSourceShortestPathBatchOp()
            .setEdgeSourceCol("source")
            .setEdgeTargetCol("target")
            .setEdgeWeightCol("weight")
            .setSourcePoint("1");

        BatchOperator res = op.linkFrom(inData);


//        res.print();
        List<Row> list = res.collect();
        Assert.assertEquals(list.size(), 11);

        new SingleSourceShortestPathBatchOp(new Params());
    }

}