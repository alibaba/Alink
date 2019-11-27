package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.common.MLEnvironmentFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LdaTrainBatchOpTest {

    @Test
    public void testTrainAndPredict() throws Exception {

        Row[] testArray =
                new Row[]{
                        Row.of(0, "a b b c c c c c c e e f f f g h k k k"),
                        Row.of(1, "a b b b d e e e h h k"),
                        Row.of(2, "a b b b b c f f f f g g g g g g g g g i j j"),
                        Row.of(3, "a a b d d d g g g g g i i j j j k k k k k k k k k"),
                        Row.of(4, "a a a b c d d d d d d d d d e e e g g j k k k"),
                        Row.of(5, "a a a a b b d d d e e e e f f f f f g h i j j j j"),
                        Row.of(6, "a a b d d d g g g g g i i j j k k k k k k k k k"),
                        Row.of(7, "a b c d d d d d d d d d e e f g g j k k k"),
                        Row.of(8, "a a a a b b b b d d d e e e e f f g h h h"),
                        Row.of(9, "a a b b b b b b b b c c e e e g g i i j j j j j j j k k"),
                        Row.of(10, "a b c d d d d d d d d d f f g g j j j k k k"),
                        Row.of(11, "a a a a b e e e e f f f f f g h h h j")
                };

        BatchOperator data = new TableSourceBatchOp(MLEnvironmentFactory.getDefault().createBatchTable(
            Arrays.asList(testArray),
            new String[]{"id", "libsvm"}));

        LdaTrainBatchOp lda = new LdaTrainBatchOp()
            .setSelectedCol("libsvm")
            .setTopicNum(6)
            .setMethod("em")
            .setSubsamplingRate(1.0)
            .setOptimizeDocConcentration(true)
            .setNumIter(50);
        BatchOperator model = lda.linkFrom(data);
        //get logPerplexity and logLikelihood
        List res = model.getSideOutput(1).collect();
        assertEquals((Double) ((Row) res.get(0)).getField(0), 53.5, 3.0);
        LdaPredictBatchOp predictBatchOp = new LdaPredictBatchOp().setPredictionCol("pred").setSelectedCol("libsvm");
        predictBatchOp.linkFrom(model, data).print();
    }
}
