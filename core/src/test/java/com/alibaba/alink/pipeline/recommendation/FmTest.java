package com.alibaba.alink.pipeline.recommendation;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.classification.FmClassifier;
import com.alibaba.alink.pipeline.classification.FmModel;
import com.alibaba.alink.pipeline.regression.FmRegressor;

import org.junit.Test;

/**
 * @author weibo zhao
 * @date 16/06/2020 8:46 PM
 */
public class FmTest {

    @Test
    public void testClassification() throws Exception {
        BatchOperator trainData = new MemSourceBatchOp(
            new Object[][] {
                {"0:1.1 5:2.0", 1.0},
                {"1:2.1 6:3.1", 1.0},
                {"2:3.1 7:2.2", 1.0},
                {"3:1.2 8:3.2", 0.0},
                {"4:1.2 9:4.2", 0.0}
            },
            new String[] {"vec", "label"});
        FmClassifier adagrad = new FmClassifier()
            .setVectorCol("vec")
            .setLabelCol("label")
            .setNumEpochs(10)
            .setInitStdev(0.01)
            .setLearnRate(0.1)
            .setEpsilon(0.0001)
            .setPredictionCol("pred")
            .setPredictionDetailCol("details")
            .enableLazyPrintModelInfo()
            .enableLazyPrintTrainInfo();

        FmModel model = adagrad.fit(trainData);
        BatchOperator result = model.transform(trainData);

        result.print();
    }

    @Test
    public void testRegression() throws Exception {
        BatchOperator trainData = new MemSourceBatchOp(
            new Object[][] {
                {"0:1.1 5:2.0", 1.0},
                {"1:2.1 6:3.1", 1.0},
                {"2:3.1 7:2.2", 1.0},
                {"3:1.2 8:3.2", 0.0},
                {"4:1.2 9:4.2", 0.0}
            },
            new String[] {"vec", "label"});
        FmRegressor adagrad = new FmRegressor()
            .setVectorCol("vec")
            .setLabelCol("label")
            .setNumEpochs(10)
            .setInitStdev(0.01)
            .setLearnRate(0.1)
            .setEpsilon(0.0001)
            .setPredictionCol("pred")
            .setPredictionDetailCol("details")
            .enableLazyPrintModelInfo()
            .enableLazyPrintTrainInfo();

        FmModel model = adagrad.fit(trainData);
        BatchOperator result = model.transform(trainData);

        result.print();
    }
}
