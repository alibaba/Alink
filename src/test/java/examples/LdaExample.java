package examples;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.pipeline.clustering.Lda;
import com.alibaba.alink.pipeline.clustering.LdaModel;
import org.junit.Test;

import java.util.Arrays;

public class LdaExample {

    Row[] testArray =
            new Row[]{
                    Row.of(new Object[]{0, "0:1 1:2 2:6 3:0 4:2 5:3 6:1 7:1 8:0 9:0 10:3"}),
                    Row.of(new Object[]{1, "0:1 1:3 2:0 3:1 4:3 5:0 6:0 7:2 8:0 9:0 10:1"}),
                    Row.of(new Object[]{2, "0:1 1:4 2:1 3:0 4:0 5:4 6:9 7:0 8:1 9:2 10:0"}),
                    Row.of(new Object[]{3, "0:2 1:1 2:0 3:3 4:0 5:0 6:5 7:0 8:2 9:3 10:9"}),
                    Row.of(new Object[]{4, "0:3 1:1 2:1 3:9 4:3 5:0 6:2 7:0 8:0 9:1 10:3"}),
                    Row.of(new Object[]{5, "0:4 1:2 2:0 3:3 4:4 5:5 6:1 7:1 8:1 9:4 10:0"}),
                    Row.of(new Object[]{6, "0:2 1:1 2:0 3:3 4:0 5:0 6:5 7:0 8:2 9:2 10:9"}),
                    Row.of(new Object[]{7, "0:1 1:1 2:1 3:9 4:2 5:1 6:2 7:0 8:0 9:1 10:3"}),
                    Row.of(new Object[]{8, "0:4 1:4 2:0 3:3 4:4 5:2 6:1 7:3 8:0 9:0 10:0"}),
                    Row.of(new Object[]{9, "0:2 1:8 2:2 3:0 4:3 5:0 6:2 7:0 8:2 9:7 10:2"}),
                    Row.of(new Object[]{10, "0:1 1:1 2:1 3:9 4:0 5:2 6:2 7:0 8:0 9:3 10:3"}),
                    Row.of(new Object[]{11, "0:4 1:1 2:0 3:0 4:4 5:5 6:1 7:3 8:0 9:1 10:0"})
            };

    Table inOp = MLEnvironmentFactory.getDefault().createBatchTable(
            Arrays.asList(testArray),
            new String[]{"id", "libsvm"});

    @Test
    public void testTrainAndPredict() throws Exception {


        Lda lda = new Lda()
            .setSelectedCol("libsvm")
            .setTopicNum(5)
            .setNumIter(100)
            .setMethod("em")
            .setReservedCols(new String[]{"id"})
            .setPredictionCol("predcol");

        LdaModel model = lda.fit(inOp);
        TestUtil.printTable(model.transform(inOp));

    }

    @Test
    public void testTrainAndPredict2() throws Exception {


        Lda lda = new Lda()
            .setSelectedCol("libsvm")
            .setTopicNum(5)
            .setMethod("online")
            .setSubsamplingRate(1.0)
            .setOptimizeDocConcentration(true)
            .setNumIter(100)
            .setReservedCols(new String[]{"id"})
            .setPredictionCol("predcol");

        LdaModel model = lda.fit(inOp);
        TestUtil.printTable(model.transform(inOp));

    }
}