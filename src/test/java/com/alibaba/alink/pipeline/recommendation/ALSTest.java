package com.alibaba.alink.pipeline.recommendation;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.recommendation.AlsPredictBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.operator.common.recommendation.AlsModelData;
import com.alibaba.alink.operator.common.recommendation.AlsModelDataConverter;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for {@link ALS}.
 */
public class ALSTest {
    private Row[] rows1 = new Row[]{
        Row.of(1L, 1L, 0.6),
        Row.of(2L, 2L, 0.8),
        Row.of(2L, 3L, 0.6),
        Row.of(3L, 1L, 0.6),
        Row.of(3L, 2L, 0.3),
        Row.of(3L, 3L, 0.4),
    };

    private Row[] rows2 = new Row[]{
        Row.of(1L, 1L, 6.0),
        Row.of(1L, 2L, -10.0),
        Row.of(1L, 3L, -5.0),
        Row.of(2L, 1L, 0.0),
        Row.of(2L, 2L, 8.0),
        Row.of(2L, 3L, 6.0),
        Row.of(3L, 1L, 6.0),
        Row.of(3L, 2L, 3.0),
        Row.of(3L, 3L, 0.1),
    };

    @Test
    public void testExplicit() throws Exception {
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
            .setPredictionCol("predicted_rating");

        ALSModel model = als.fit(data);
        checkRatingError(model.transform(data));

        checkRatingError(new AlsPredictBatchOp().setUserCol("user").setItemCol("item").setPredictionCol("predicted_rating")
            .linkFrom(BatchOperator.fromTable(model.getModelData()), data));
    }

    private static void checkRatingError(BatchOperator prediction) {
        RegressionMetrics metrics = new EvalRegressionBatchOp()
            .setLabelCol("rating")
            .setPredictionCol("predicted_rating")
            .linkFrom(prediction)
            .collectMetrics();

        Assert.assertTrue(metrics.getMae() < 0.02);
    }

    private static void checkNonNegative(float[][] factors) {
        for (float[] factor : factors) {
            for (float aFactor : factor) {
                Assert.assertTrue(aFactor >= 0.);
            }
        }
    }

    @Test
    public void testNonNegative() throws Exception {
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
            .setNonnegative(true)
            .setPredictionCol("predicted_rating");

        ALSModel model = als.fit(data);
        checkRatingError(model.transform(data));

        AlsModelData modelData = new AlsModelDataConverter("user", "item")
            .load(BatchOperator.fromTable(model.getModelData()).collect());

        checkNonNegative(modelData.userFactors);
        checkNonNegative(modelData.itemFactors);
    }

    @Test
    public void testUnSeenUser() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();
        BatchOperator data = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[]{"user", "item", "rating"}));

        Row[] predictRows = new Row[]{
            Row.of(4L, 1L),
        };
        BatchOperator predictData = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(predictRows, new String[]{"user", "item"}));

        ALS als = new ALS()
            .setUserCol("user")
            .setItemCol("item")
            .setRateCol("rating")
            .setLambda(0.01)
            .setRank(10)
            .setNumIter(10)
            .setNonnegative(true)
            .setPredictionCol("predicted_rating");

        ALSModel model = als.fit(data);
        Row pred1 = (Row) model.transform(predictData).collect().get(0);
        Assert.assertEquals(pred1.getField(0), 4L);
        Assert.assertEquals(pred1.getField(1), 1L);
        Assert.assertTrue(pred1.getField(2) == null);

//        Assert.assertEquals(
//            BatchOperator.fromTable(model.recommendForUserSubset(predictData.getOutput(), 100)).collect().size(), 0);

        Row pred3 = new AlsPredictBatchOp().setUserCol("user").setItemCol("item").setPredictionCol("predicted_rating")
            .linkFrom(BatchOperator.fromTable(model.getModelData()), predictData).collect().get(0);
        Assert.assertEquals(pred3.getField(0), 4L);
        Assert.assertEquals(pred3.getField(1), 1L);
        Assert.assertTrue(pred3.getField(2) == null);
    }

    @Test
    public void recommendTest() throws Exception {

    }

    @Test
    public void testImplicit() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();
        BatchOperator data = BatchOperator.fromTable(
            MLEnvironmentFactory.getDefault().createBatchTable(rows2, new String[]{"user", "item", "rating"}));

        ALS als = new ALS()
            .setUserCol("user")
            .setItemCol("item")
            .setRateCol("rating")
            .setLambda(0.01)
            .setRank(10)
            .setNumIter(10)
            .setImplicitPrefs(true)
            .setPredictionCol("predicted_rating");

        BatchOperator prediction = als.fit(data).transform(data);
        prediction = prediction.select("(case rating > 0 when true then 1 else 0 end) as rating, predicted_rating");
        checkRatingError(prediction);
    }
}