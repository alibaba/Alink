package com.alibaba.alink.pipeline.classification;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class LogisticRegTest {

    AlgoOperator getData(boolean isBatch) {
        Row[] array = new Row[] {
            Row.of(new Object[] {"$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1}),
            Row.of(new Object[] {"$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1}),
            Row.of(new Object[] {"$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1}),
            Row.of(new Object[] {"$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1}),
            Row.of(new Object[] {"$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0}),
            Row.of(new Object[] {"$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0}),
            Row.of(new Object[] {"$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0}),
            Row.of(new Object[] {"$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0})
        };

        if (isBatch) {
            return new MemSourceBatchOp(
                Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"});
        } else {
            return new MemSourceStreamOp(
                Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"});
        }
    }

    @Test
    public void pipelineTestBatch() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();

        String[] xVars = new String[] {"f0", "f1", "f2", "f3"};
        String yVar = "labels";
        String vectorName = "vec";
        String svectorName = "svec";
        LogisticRegression lr = new LogisticRegression()
            .setLabelCol(yVar)
            .setFeatureCols(xVars)
            .setPredictionCol("lrpred");

        LogisticRegression vectorLr = new LogisticRegression()
            .setLabelCol(yVar)
            .setVectorCol(vectorName)
            .setPredictionCol("vlrpred");

        LogisticRegression sparseVectorLr = new LogisticRegression()
            .setLabelCol(yVar)
            .setVectorCol(svectorName)
            .setPredictionCol("svlrpred");

        Pipeline plLr = new Pipeline().add(lr).add(vectorLr).add(sparseVectorLr);
        BatchOperator trainData = (BatchOperator)getData(true);
        PipelineModel model = plLr.fit(trainData);
        BatchOperator result = model.transform(trainData).select(
            new String[] {"labels", "lrpred", "vlrpred", "svlrpred"});

        List<Row> data = result.collect();
        for (Row row : data) {
            for (int i = 1; i < 3; ++i) {
                Assert.assertEquals(row.getField(0), row.getField(i));
            }
        }
    }
}
