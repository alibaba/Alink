package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.NaiveBayesPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

public class NaiveBayesTest {
    public static AlgoOperator getData(boolean isBatch) {
        Row[] array = new Row[] {
            Row.of(1., "a", 1., 1., 1., 2, "l1"),
            Row.of(1., "a", 1., 0., 1., 2, "l1"),
            Row.of(1., "b", 0., 1., 1., 3, "l1"),
            Row.of(1., "d", 0., 1., 1.5, 2, "l1"),
            Row.of(2., "c", 1.5, 1., 0.5, 3, "l0"),
            Row.of(1., "a", 1., 1.5, 0., 1, "l0"),
            Row.of(2., "d", 1., 1., 0., 1, "l0"),
        };

        if (isBatch) {
            return new MemSourceBatchOp(
                Arrays.asList(array), new String[] {"weight", "f0", "f1", "f2", "f3", "f4", "label"});
        } else {
            return new MemSourceStreamOp(
                Arrays.asList(array), new String[] {"weight", "f0", "f1", "f2", "f3", "f4", "label"});
        }
    }

    @Test
    public void testnaiveBayes() throws Exception {
        String[] feature = new String[] {"f0", "f1", "f2", "f3", "f4"};
        String label = "label";
        BatchOperator batchData = (BatchOperator) getData(true);
        NaiveBayesTrainBatchOp op = new NaiveBayesTrainBatchOp()
            .setCategoricalCols("f0", "f4").setSmoothing(0.0).setWeightCol("weight")
            .setFeatureCols(feature).setLabelCol(label).linkFrom(batchData);
        op.getModelInfoBatchOp().lazyPrintModelInfo();
        op.lazyPrint(-1);
        NaiveBayesPredictBatchOp predict = new NaiveBayesPredictBatchOp()
            .setPredictionCol("predict").setPredictionDetailCol("detail").setReservedCols("label");
        BatchOperator res = predict.linkFrom(op, batchData);
        res.select(new String[]{"label", "predict"}).lazyCollect();
        StreamOperator streamData = (StreamOperator) getData(false);
        new NaiveBayesPredictStreamOp(op).setPredictionCol("predict").linkFrom(streamData).print();
        StreamOperator.execute();
        new NaiveBayes()
            .setFeatureCols(feature).setLabelCol(label).setPredictionCol("predict")
            .fit(batchData).transform(batchData).collect();
    }

}
