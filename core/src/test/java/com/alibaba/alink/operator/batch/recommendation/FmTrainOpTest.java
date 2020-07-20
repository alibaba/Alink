package com.alibaba.alink.operator.batch.recommendation;

import java.util.function.Consumer;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.FmClassifierTrainBatchOp;
import com.alibaba.alink.operator.batch.dataproc.JsonValueBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.regression.FmRegressorPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.FmRegressorTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.fm.FmModelInfo;
import com.alibaba.alink.operator.common.fm.FmPredictBatchOp;
import com.alibaba.alink.pipeline.classification.FmClassifier;
import com.alibaba.alink.pipeline.classification.FmModel;

import org.junit.Test;

/**
 * @author weibo zhao
 * @date 16/06/2020 8:46 PM
 */
public class FmTrainOpTest {
    @Test
    public void testFmRegression() throws Exception {
        BatchOperator trainData = new MemSourceBatchOp(
            new Object[][] {
                {"1.1 1.0", 1.0},
                {"1.1 1.1", 1.0},
                {"1.1 1.2", 1.0},
                {"0.0 3.2", 0.0},
                {"0.0 4.2", 0.0}
            },
            new String[] {"vec", "label"});
        FmRegressorTrainBatchOp adagrad = new FmRegressorTrainBatchOp()
            .setVectorCol("vec")
            .setLabelCol("label")
            .setNumEpochs(10)
            .setNumFactor(5)
            .setInitStdev(0.01)
            .setLearnRate(0.5)
            .setEpsilon(0.0001)
            .linkFrom(trainData);
        adagrad.lazyPrintModelInfo();
        new FmRegressorPredictBatchOp().setVectorCol("vec").setPredictionCol("pred")
            .setPredictionDetailCol("details")
            .linkFrom(adagrad, trainData)
            .print();
    }

    @Test
    public void testFmClasificationSparse() throws Exception {
        //MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();
        BatchOperator trainData = new MemSourceBatchOp(
            new Object[][] {
                {"1:1.1 3:2.0", 1.0},
                {"2:2.1 10:3.1", 1.0},
                {"3:3.1 7:2.2", 1.0},
                {"1:1.2 5:3.2", 0.0},
                {"4:1.2 7:4.2", 0.0}
            },
            new String[] {"vec", "label"});
        FmClassifierTrainBatchOp adagrad = new FmClassifierTrainBatchOp()
            .setVectorCol("vec")
            .setLabelCol("label")
            .setNumEpochs(10)
            .setInitStdev(0.01)
            .setLearnRate(0.01)
            .setEpsilon(0.0001)
            .linkFrom(trainData);

        adagrad.lazyPrintModelInfo();
        adagrad.lazyCollectModelInfo(new Consumer<FmModelInfo>() {
            @Override
            public void accept(FmModelInfo modelinfo) {
                String[] names = modelinfo.getColNames();
                String tast = modelinfo.getTask();
                double[][] factors = modelinfo.getFactors();
                int[] dim = modelinfo.getDim();
                int[] pos = modelinfo.getFiledPos();
                int size = modelinfo.getVectorSize();
            }
        });

        BatchOperator result = new FmPredictBatchOp().setVectorCol("vec").setPredictionCol("pred")
            .setPredictionDetailCol("details")
            .linkFrom(adagrad, trainData);

        new EvalBinaryClassBatchOp()
            .setLabelCol("label")
            .setPredictionDetailCol("details")
            .linkFrom(result)
            .link(new JsonValueBatchOp()
                .setSelectedCol("Data")
                .setReservedCols(new String[] {"Statistics"})
                .setOutputCols(new String[] {"Accuracy", "AUC", "ConfusionMatrix"})
                .setJsonPath(new String[] {"$.Accuracy", "$.AUC", "$.ConfusionMatrix"}))
            .print();
    }
}
