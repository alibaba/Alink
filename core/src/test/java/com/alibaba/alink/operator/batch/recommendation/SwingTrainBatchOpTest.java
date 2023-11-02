package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.recommendation.SwingSimilarItemsRecommender;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SwingTrainBatchOpTest extends AlinkTestBase {
    Row[] rows = new Row[]{
        Row.of("a1", "11L", 2.2),
        Row.of("a1", "12L", 2.0),
        Row.of("a1", "16L", 2.0),
        Row.of("a2", "11L", 2.0),
        Row.of("a2", "12L", 2.0),
        Row.of("a3", "12L", 2.0),
        Row.of("a3", "13L", 2.0),
        Row.of("a4", "13L", 2.0),
        Row.of("a4", "14L", 2.0),
        Row.of("a4", "12L", 2.0),
        Row.of("a5", "14L", 2.0),
        Row.of("a5", "15L", 2.0),
        Row.of("a5", "13L", 2.0),
        Row.of("a6", "15L", 2.0),
        Row.of("a6", "16L", 2.0),
        Row.of("a6", "14L", 2.0),
        Row.of("a6", "12L", 2.0),
    };

    String[] colNames = new String[]{"user", "item", "weight"};
    BatchOperator swingData = new MemSourceBatchOp(
        Arrays.asList(rows), colNames);

    @Test
    public void testRecomm() throws Exception {
        AlinkGlobalConfiguration.setPrintProcessInfo(true);

        SwingTrainBatchOp swing = new SwingTrainBatchOp()
            .setUserCol("user")
            .setItemCol("item")
            .setAlpha(10)
            .setUserAlpha(10)
            .setUserBeta(0.4)
            .setMinUserItems(1)
            .setMaxUserItems(10)
            .setMaxItemNumber(10)
            .setResultNormalize(false)
            .linkFrom(swingData);
        swing.lazyPrint(100);
        SwingRecommBatchOp recomm = new SwingRecommBatchOp()
            .setItemCol("item")
            .setRecommCol("recomm")
            .linkFrom(swing, swingData);
        recomm.lazyPrint(-1);
        SwingSimilarItemsRecommender recommender = new SwingSimilarItemsRecommender()
            .setItemCol("item")
            .setRecommCol("recomm")
            .setModelData(swing);
        List<Row> res = recommender.transform(swingData).collect();
        System.out.println(res);
        Assert.assertEquals(res.size(), 17);
    }

}