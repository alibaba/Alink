package com.alibaba.alink.operator.batch.recommendation;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.recommendation.ALS;
import com.alibaba.alink.pipeline.recommendation.ALSModel;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class AlsTopKPredictBatchOpTest {
    private Row[] rows1 = new Row[]{
        Row.of(1L, 1L, 0.6),
        Row.of(2L, 2L, 0.8),
        Row.of(2L, 3L, 0.6),
        Row.of(3L, 1L, 0.6),
        Row.of(3L, 2L, 0.3),
        Row.of(3L, 3L, 0.4),
    };

    private Row[] rows2 = new Row[]{
        Row.of(0L),
        Row.of(1L),
        Row.of(2L),
        Row.of(2L),
        Row.of(3L),
    };

    @Test
    public void testRecommendForUsers() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();
        BatchOperator data = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[]{"user", "item", "rating"}));
        BatchOperator predData = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows2, new String[]{"user"}));

        ALS als = new ALS()
            .setUserCol("user")
            .setItemCol("item")
            .setRateCol("rating")
            .setLambda(0.01)
            .setRank(10)
            .setNumIter(10)
            .setPredictionCol("recommendations");

        ALSModel model = als.fit(data);
        BatchOperator modelData = BatchOperator.fromTable(model.getModelData());

        BatchOperator pred = new AlsTopKPredictBatchOp()
            .setUserCol("user").setTopK(2).setPredictionCol("topk").linkFrom(modelData, predData);

        pred.print();
        Assert.assertEquals(pred.count(), 4);
    }

}