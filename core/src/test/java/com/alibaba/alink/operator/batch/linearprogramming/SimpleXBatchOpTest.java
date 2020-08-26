package com.alibaba.alink.operator.batch.linearprogramming;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import junit.framework.TestCase;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class SimpleXBatchOpTest extends TestCase {
    double calculate(Row[] r1, Row[] r2, Row[] r3, Row[] r4, Row[] r5) {
        BatchOperator op1 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r1, new String[]{"x0", "x1", "relation", "b"}));
        BatchOperator op2 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r2, new String[]{"x", "c"}));
        BatchOperator op3 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r3, new String[]{"x", "upperBounds"}));
        BatchOperator op4 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r4, new String[]{"x", "lowerBounds"}));
        BatchOperator op5 = BatchOperator.fromTable(MLEnvironmentFactory.getDefault()
                .createBatchTable(r5, new String[]{"x",}));

        SimpleXBatchOp test = new SimpleXBatchOp();
        test.setMaxIter(8);
        BatchOperator.setParallelism(2);
        double result = (double) test.linkFrom(op1, op2, op3, op4, op5).collect().get(0).getField(0);
        return result;
    }

    @Test
    public void test1() throws Exception {
        Row[] r1 = new Row[]{Row.of(30.0, 20.0, "le", 160.0), Row.of(5.0, 1.0, "le", 15.0), Row.of(1.0, 0.0, "le", 4.0)};
        Row[] r2 = new Row[]{Row.of(0, -5.0), Row.of(1, -2.0)};
        Row[] r3 = new Row[]{Row.of(1, 300.0)};
        Row[] r4 = new Row[]{Row.of(0, -3.0)};
        Row[] r5 = new Row[]{Row.of(0)};
        double result = calculate(r1, r2, r3, r4, r5);
        Assert.assertEquals(result, -20.00, 10e-4);
    }

    @Test
    public void test2() throws Exception {
        Row[] r1 = new Row[]{Row.of(-3.0, 1.0, "le", 6.0), Row.of(1.0, 2.0, "le", 4.0)};
        Row[] r2 = new Row[]{Row.of(0, -1.0), Row.of(1, 4.0)};
        Row[] r3 = new Row[]{Row.of(1, 300.0)};
        Row[] r4 = new Row[]{Row.of(1, -3.0)};
        Row[] r5 = new Row[]{Row.of(0)};
        double result = calculate(r1,r2,r3,r4,r5);
        Assert.assertEquals(result, -22.00, 10e-4);
    }

    @Test
    public void test3() throws Exception {
        Row[] r1 = new Row[]{Row.of(-1.0, 1.0, "le", 2.0), Row.of(1.0, 1.0, "le", 4.0)};
        Row[] r2 = new Row[]{Row.of(0, 1.0), Row.of(1, -2.0)};
        Row[] r3 = new Row[]{Row.of(1, 300.0)};
        Row[] r4 = new Row[]{Row.of(1, -10.0)};
        Row[] r5 = new Row[]{Row.of(0)};
        double result = calculate(r1, r2, r3, r4, r5);
        Assert.assertEquals(result, -5.00, 10e-4);
    }

    @Test
    public void test4() throws Exception {
        Row[] r1 = new Row[]{Row.of(-3.0, 1.0, "le", 6.0), Row.of(1.0, 2.0, "le", 4.0)};
        Row[] r2 = new Row[]{Row.of(0, 4.0), Row.of(1, -1.0)};
        Row[] r3 = new Row[]{Row.of(0, 300.0)};
        Row[] r4 = new Row[]{Row.of(1, -3.0)};
        Row[] r5 = new Row[]{Row.of(0)};
        double result = calculate(r1,r2,r3,r4,r5);
        System.out.println(result);
    }

    @Test
    public void test5() throws Exception {
        Row[] r1 = new Row[]{Row.of(-1.0, 1.0, "le", 2.0), Row.of(1.0, 1.0, "le", 4.0)};
        Row[] r2 = new Row[]{Row.of(0, 1.0), Row.of(1, -2.0)};
        Row[] r3 = new Row[]{Row.of(0, 300.0)};
        Row[] r4 = new Row[]{Row.of(1, -10.0)};
        Row[] r5 = new Row[]{Row.of(0)};
        double result = calculate(r1,r2,r3,r4,r5);
        System.out.println(result);
    }
}