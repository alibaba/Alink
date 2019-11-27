package com.alibaba.alink.operator.batch.recommendation;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.pipeline.recommendation.ALS;
import com.alibaba.alink.pipeline.recommendation.ALSModel;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class AlsPredictBatchOpTest {
    private Row[] rows1 = new Row[]{
        Row.of(1L, 1L, 0.6),
        Row.of(2L, 2L, 0.8),
        Row.of(2L, 3L, 0.6),
        Row.of(3L, 1L, 0.6),
        Row.of(3L, 2L, 0.3),
        Row.of(3L, 3L, 0.4),
    };

    private static void checkRatingError(BatchOperator prediction) {
        RegressionMetrics metrics = new EvalRegressionBatchOp()
            .setLabelCol("rating")
            .setPredictionCol("predicted_rating")
            .linkFrom(prediction)
            .collectMetrics();

        Assert.assertTrue(metrics.getMae() < 0.02);
    }

    @Test
    public void testRating() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();
        BatchOperator data = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[]{"user", "item", "rating"}));

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

        BatchOperator pred = new AlsPredictBatchOp()
            .setUserCol("user").setItemCol("item").setPredictionCol("predicted_rating").linkFrom(modelData, data);

        checkRatingError(pred);
    }

}