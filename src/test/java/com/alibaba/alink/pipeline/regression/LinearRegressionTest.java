package com.alibaba.alink.pipeline.regression;

import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class LinearRegressionTest {
    Row[] vecrows = new Row[] {
        Row.of("$3$0:1.0 1:7.0 2:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 16.8),
        Row.of("$3$0:1.0 1:3.0 2:3.0", "1.0 3.0 3.0", 1.0, 3.0, 3.0, 6.7),
        Row.of("$3$0:1.0 1:2.0 2:4.0", "1.0 2.0 4.0", 1.0, 2.0, 4.0, 6.9),
        Row.of("$3$0:1.0 1:3.0 2:4.0", "1.0 3.0 4.0", 1.0, 3.0, 4.0, 8.0)
    };
    String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "label"};
    BatchOperator vecdata = new MemSourceBatchOp(Arrays.asList(vecrows), veccolNames);
    StreamOperator svecdata = new MemSourceStreamOp(Arrays.asList(vecrows), veccolNames);

    @Test
    public void regressionPipelineTest() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().setParallelism(1);
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().getConfig().disableSysoutLogging();

        String[] xVars = new String[] {"f0", "f1", "f2"};
        String yVar = "label";
        String vec = "vec";
        String svec = "svec";
        LinearRegression linear = new LinearRegression()
            .setLabelCol(yVar)
            .setFeatureCols(xVars)
            .setPredictionCol("linpred");

        LinearRegression vlinear = new LinearRegression()
            .setLabelCol(yVar)
            .setVectorCol(vec)
            .setPredictionCol("vlinpred");

        LinearRegression svlinear = new LinearRegression()
            .setLabelCol(yVar)
            .setVectorCol(svec)
            .setPredictionCol("svlinpred");

        Pipeline pl = new Pipeline().add(linear).add(vlinear).add(svlinear);
        PipelineModel model = pl.fit(vecdata);

        BatchOperator result = model.transform(vecdata).select(
            new String[] {"label", "linpred", "vlinpred", "svlinpred"});

        List<Row> data = result.collect();
        for (Row row : data) {
            if ((double)row.getField(0) == 16.8000) {
                Assert.assertEquals((double)row.getField(1), 16.814789059973744, 0.01);
                Assert.assertEquals((double)row.getField(2), 16.814789059973744, 0.01);
                Assert.assertEquals((double)row.getField(3), 16.814788687904162, 0.01);
            } else if ((double)row.getField(0) == 6.7000) {
                Assert.assertEquals((double)row.getField(1), 6.773942836224718, 0.01);
                Assert.assertEquals((double)row.getField(2), 6.773942836224718, 0.01);
                Assert.assertEquals((double)row.getField(3), 6.773943529327923, 0.01);
            }
        }

        // below is stream test code
        model.transform(svecdata).print();
        StreamOperator.execute();
    }

}
