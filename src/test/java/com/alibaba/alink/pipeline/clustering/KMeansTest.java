package com.alibaba.alink.pipeline.clustering;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.httpsrc.Iris;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.common.evaluation.ClusterMetrics;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class KMeansTest {
    @Test
    public void test() throws Exception {
        Row[] rows = new Row[] {
            Row.of("0 0 0"),
            Row.of("0.1 0.1 0.1"),
            Row.of("0.2 0.2 0.2"),
            Row.of("9 9 9"),
            Row.of("9.1 9.1 9.1"),
            Row.of("9.2 9.2 9.2")
        };

        Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"vector"});
        Table dataStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, new String[] {"vector"});

        KMeans kMeans = new KMeans()
            .setVectorCol("vector")
            .setPredictionCol("pred")
            .setPredictionDistanceCol("distance")
            .setK(2);

        PipelineModel model = new Pipeline().add(kMeans).fit(data);

        Table res = model.transform(data);

        List<Double> list = MLEnvironmentFactory.getDefault().getBatchTableEnvironment().toDataSet(
            res.select("distance"), Double.class)
            .collect();
        double[] actual = new double[] {0.173, 0, 0.173, 0.173, 0, 0.173};
        for (int i = 0; i < actual.length; i++) {
            Assert.assertEquals(list.get(i), actual[i], 0.01);
        }

        res = model.transform(dataStream);

        DataStreamConversionUtil.fromTable(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID, res).print();

        MLEnvironmentFactory.getDefault().getStreamExecutionEnvironment().execute();
    }
}