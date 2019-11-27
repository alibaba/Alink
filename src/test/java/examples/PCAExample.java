package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.pipeline.feature.PCA;
import com.alibaba.alink.pipeline.feature.PCAModel;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class PCAExample {
    private Table trainSparseBatch;
    private Table predictSparseBatch;
    private Table predictSparseStream;

    @Before
    public void setUp() throws Exception {
        genSparseTensor();
    }

    @Test
    public void testPipeline() throws Exception {
        PCA pca = new PCA()
            .setK(3)
            .setCalculationType("CORR")
            .setPredictionCol("pred")
            .setReservedCols(new String[]{"id"})
            .setVectorCol("matrix");

        PCAModel model = pca.fit(trainSparseBatch);
        TestUtil.printTable(model.transform(predictSparseBatch));
        TestUtil.printTable(model.transform(predictSparseStream));
    }

    @Test
    public void test2() throws Exception {
        Row[] testArray =
            new Row[]{
                Row.of(new Object[]{"$2$0:1.0 1:2.0"}),
                Row.of(new Object[]{"$2$0:-1.0 1:-3.0"}),
                Row.of(new Object[]{"$2$0:4.0 1:2.0"}),
                Row.of(new Object[]{"$2$0:4.0 1:2.0"}),
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{selectedColName};

        Table data = MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);

        PCA pca = new PCA()
            .setK(1)
            .setCalculationType("CORR")
            .setPredictionCol("pred")
            .setVectorCol("vec");

        PCAModel model = pca.fit(data);
        TestUtil.printTable(model.transform(data));
    }

    void genSparseTensor() {
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
        predictSparseStream = MLEnvironmentFactory.getDefault().createStreamTable(rows, colNames);
    }
}