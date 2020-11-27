package com.alibaba.alink.operator.batch.recommendation;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import org.junit.Test;

public class FmRecommImplicitTest {
    private static final String PATH = "http://alink-testdata.oss-cn-hangzhou-zmf.aliyuncs.com/csv/lightfm/";
    private static final String TRAIN_FILE = "train.csv";
    private static final String TEST_FILE = "test.csv";
    private static final String ITEM_FEATURES_FILE = "item_features.csv";

    static BatchOperator trainData = new CsvSourceBatchOp()
            .setFilePath(PATH + TRAIN_FILE).setSchemaStr("user bigint, item bigint, label bigint");
    static BatchOperator testData = new CsvSourceBatchOp()
            .setFilePath(PATH + TEST_FILE).setSchemaStr("user bigint, item bigint, label bigint")
            .select("user,item")
            .link(new NegativeItemSamplingBatchOp());
    static BatchOperator itemFeatures = new CsvSourceBatchOp()
            .setFilePath(PATH + ITEM_FEATURES_FILE).setSchemaStr("item bigint, features string");

    private static void eval(BatchOperator pred) {
        pred = pred.select(
                "label, concat('{\"0\":', cast((1-p) as varchar), ',\"1\":', cast(p as varchar), '}') as p_detail");

        EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
                .setLabelCol("label").setPredictionDetailCol("p_detail")
                .linkFrom(pred);

        BinaryClassMetrics metrics = eval.collectMetrics();
        System.out.println(
                String.format("auc=%f,acc=%f,f1=%f", metrics.getAuc(), metrics.getAccuracy(), metrics.getF1()));
    }

    @Test
    public void test() throws Exception {
        FmRecommBinaryImplicitTrainBatchOp fmTrain = new FmRecommBinaryImplicitTrainBatchOp()
                .setUserCol("user")
                .setItemCol("item")
                .setItemFeatureCols(new String[]{"features"})
                .setLearnRate(0.1)
                .setNumEpochs(10)
                .setLambda0(0.01)
                .setLambda1(0.01)
                .setLambda2(0.01)
                .setWithIntercept(true)
                .setWithLinearItem(true)
                .setNumFactor(5);

        fmTrain.linkFrom(trainData, null, itemFeatures);

        FmRateRecommBatchOp predictor = new FmRateRecommBatchOp()
                .setUserCol("user").setItemCol("item")
                .setRecommCol("p");

        BatchOperator pred = predictor.linkFrom(fmTrain, testData);
        eval(pred);
    }
}
