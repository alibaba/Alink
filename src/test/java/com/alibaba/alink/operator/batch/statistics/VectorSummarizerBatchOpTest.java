package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class VectorSummarizerBatchOpTest {

    @Test
    public void test() {
        Row[] testArray =
            new Row[]{
                Row.of("1.0 2.0"),
                Row.of("-1.0 -3.0"),
                Row.of("4.0 2.0"),
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{selectedColName};

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

        VectorSummarizerBatchOp summarizer = new VectorSummarizerBatchOp()
            .setSelectedCol("vec");

        summarizer.linkFrom(source);

        BaseVectorSummary srt = summarizer.collectVectorSummary();

        System.out.println(srt);

        Assert.assertEquals(srt.vectorSize(), 2);
        Assert.assertEquals(srt.count(), 3);
        Assert.assertEquals(srt.max(0), 4.0, 10e-4);
        Assert.assertEquals(srt.min(0), -1.0, 10e-4);
        Assert.assertEquals(srt.mean(0), 1.3333333333333333, 10e-4);
        Assert.assertEquals(srt.variance(0), 6.333333333333334, 10e-4);
        Assert.assertEquals(srt.standardDeviation(0), 2.5166114784235836, 10e-4);
        Assert.assertEquals(srt.normL1(0), 6.0, 10e-4);
        Assert.assertEquals(srt.normL2(0), 4.242640687119285, 10e-4);
    }

}