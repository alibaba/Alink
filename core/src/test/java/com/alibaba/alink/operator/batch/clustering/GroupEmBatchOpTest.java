package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;

public class GroupEmBatchOpTest extends AlinkTestBase {
    @Test
    public void test() throws Exception{
        Row[] array3 = new Row[] {
            Row.of(0, "id_1", 2.0, 3.0),
            Row.of(0, "id_2", 2.1, 3.1),
            Row.of(0, "id_3", 200.1, 300.1),
            Row.of(0, "id_4", 200.2, 300.2),
            Row.of(1, "id_5", 200.3, 300.3),
            Row.of(1, "id_6", 200.4, 300.4),
            Row.of(1, "id_7", 200.5, 300.5),
            Row.of(0, "id_8", 200.6, 300.6),
            Row.of(1, "id_9", 2.1, 3.1),
            Row.of(1, "id_10", 2.1, 3.1),
            Row.of(1, "id_11", 2.1, 3.1),
            Row.of(0, "id_12", 2.1, 3.1),
            Row.of(1, "id_13", 2.3, 3.2),
            Row.of(1, "id_14", 2.3, 3.2),
            Row.of(0, "id_15", 2.8, 3.2),
            Row.of(1, "id_16", 300., 3.2),
            Row.of(1, "id_17", 2.2, 3.2),
            Row.of(0, "id_18", 2.4, 3.2),
            Row.of(1, "id_19", 2.5, 3.2),
            Row.of(1, "id_20", 2.5, 3.2),
            Row.of(2, "id_20", 2.5, 3.2),
            Row.of(1, "id_21", 2.1, 3.1)
        };

        MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(array3), new String[] {"group", "id", "c1", "c2"});

        GroupEmBatchOp op = new GroupEmBatchOp()
            .setIdCol("id")
            .setGroupCols(new String[] {"group"})
            .setK(3)
            .setPredictionCol("pred")
            .setEpsilon(1e-8)
            .setFeatureCols(new String[]{"c1", "c2"});

        op.linkFrom(data).print();
    }
}