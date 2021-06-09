package com.alibaba.alink.operator.batch.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.dataproc.HasHandleInvalid.HandleInvalid;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoeAndIndex.Encode;
import com.alibaba.alink.pipeline.feature.MultiHotEncoder;
import com.alibaba.alink.pipeline.feature.MultiHotEncoderModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class MultiHotTest extends AlinkTestBase {

    Row[] array = new Row[] {
        Row.of(new Object[] {"$31$01.0 11.0 21.0 301.0", "1.0 1.0 1.0 1.0", 1.0, 1.0, 1.0, 1.0, 1}),
        Row.of(new Object[] {"$31$01.0 11.0 20.0 301.0", "1.0 1.0 0.0 1.0", 1.0, 1.0, 0.0, 1.0, 1}),
        Row.of(new Object[] {"$31$01.0 10.0 21.0 301.0", "1.0 0.0 1.0 1.0", 1.0, 0.0, 1.0, 1.0, 1}),
        Row.of(new Object[] {"$31$01.0 10.0 21.0 301.0", "1.0 0.0 1.0 1.0", 1.0, 0.0, 1.0, 1.0, 1}),
        Row.of(new Object[] {"$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0}),
        Row.of(new Object[] {"$31$00.0 11.0 21.0 300.0", null, 0.0, 1.0, 1.0, 0.0, 0}),
        Row.of(new Object[] {"$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0}),
        Row.of(new Object[] {"$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0})
    };
    String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "label"};

    Row[] predArray = new Row[] {
        Row.of(new Object[] {"$31$01.0 11.0 21.0 301.0", "1.0 1.0 1.0 1.0", 1.0, 1.0, 1.0, 1.0, 1})
    };


    @Test
    public void testMultiHot() throws Exception {
        BatchOperator vecdata = new MemSourceBatchOp(Arrays.asList(array), veccolNames);
        BatchOperator predData = new MemSourceBatchOp(Arrays.asList(predArray), veccolNames);
        MultiHotTrainBatchOp mh = new MultiHotTrainBatchOp().setSelectedCols(new String[]{"svec","vec"}).setDiscreteThresholdsArray(3,6)
            .setDelimiter(" ").linkFrom(vecdata);
        mh.lazyPrintModelInfo();
        Row result = new  MultiHotPredictBatchOp().setSelectedCols(new String[]{"svec","vec"}).setHandleInvalid(HandleInvalid.KEEP)
           .setEncode(Encode.VECTOR).setOutputCols("kv1", "kv2").linkFrom(mh, predData)
           .link(new VectorAssemblerBatchOp().setSelectedCols("kv1", "kv2").setOutputCol("kv")
           .setReservedCols(new String[]{}))
           .collect().get(0);
        Assert.assertEquals(result.getField(0), "$14$1:1.0 2:1.0 4:1.0 5:1.0 11:1.0");
    }

    @Test
    public void testMultiHotPipeline() throws Exception {
        BatchOperator vecdata = new MemSourceBatchOp(Arrays.asList(array), veccolNames);
        MultiHotEncoderModel mh = new MultiHotEncoder().setSelectedCols(new String[]{"svec","vec"}).setHandleInvalid(HandleInvalid.SKIP)
            .setDelimiter(" ").setOutputCols("kv").fit(vecdata);
        mh.transform(vecdata).print();
    }
}
