package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.VectorSummarizerBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PCATest {

    private Table trainSparseBatch;
    private Table predictSparseBatch;

    @Before
    public void setUp() throws Exception {
        genSparseTensor();
    }

    @Test
    public void testPipeline() throws Exception {
        String[] colNames = new String[] {"id", "vec"};

        Object[][] data = new Object[][] {
            {1, "0.1 0.2 0.3 0.4"},
            {2, "0.2 0.1 0.2 0.6"},
            {3, "0.2 0.3 0.5 0.4"},
            {4, "0.3 0.1 0.3 0.7"},
            {5, "0.4 0.2 0.4 0.4"}
        };

        MemSourceBatchOp source = new MemSourceBatchOp(data, colNames);

        PCA pca = new PCA()
            .setK(3)
            .setCalculationType("CORR")
            .setPredictionCol("pred")
            .setReservedCols("id")
            .setVectorCol("vec");

        PCAModel model = pca.fit(source);
        BatchOperator predict = model.transform(source);

        VectorSummarizerBatchOp summarizerOp = new VectorSummarizerBatchOp()
            .setSelectedCol("pred");

        summarizerOp.linkFrom(predict);

        BaseVectorSummary summary = summarizerOp.collectVectorSummary();

        Assert.assertEquals(4.840575043553453, Math.abs(summary.sum().get(0)), 10e-4);

    }

    private void genSparseTensor() {
        int row = 100;
        int col = 10;
        Random random = new Random(2018L);

        String[] colNames = new String[2];
        colNames[0] = "id";
        colNames[1] = "matrix";

        int[] indices = new int[col];
        for (int i = 0; i < col; i++) {
            indices[i] = i;
        }

        List<Row> rows = new ArrayList<>();
        double[] data = new double[col];
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                data[j] = random.nextDouble();
            }
            SparseVector tensor = new SparseVector(col, indices, data);

            rows.add(Row.of(i, VectorUtil.toString(tensor)));
        }

        trainSparseBatch = MLEnvironmentFactory.getDefault().createBatchTable(rows, colNames);
        predictSparseBatch = MLEnvironmentFactory.getDefault().createBatchTable(rows, colNames);
    }
}