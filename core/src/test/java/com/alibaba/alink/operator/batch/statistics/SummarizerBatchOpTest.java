package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SummarizerBatchOpTest {

    @Test
    public void test() {
        Row[] testArray =
            new Row[]{
                Row.of("a", 1L, 1, 2.0, true),
                Row.of(null, 2L, 2, -3.0, true),
                Row.of("c", null, null, 2.0, false),
                Row.of("a", 0L, 0, null, null),
            };

        String[] colNames = new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"};

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

        SummarizerBatchOp summarizer = new SummarizerBatchOp()
            .setSelectedCols("f_double", "f_int");

        summarizer.linkFrom(source);

        TableSummary srt = summarizer.collectSummary();

        System.out.println(srt);

        Assert.assertEquals(srt.getColNames().length, 2);
        Assert.assertEquals(srt.count(), 4);
        Assert.assertEquals(srt.numMissingValue("f_double"), 1, 10e-4);
        Assert.assertEquals(srt.numValidValue("f_double"), 3, 10e-4);
        Assert.assertEquals(srt.max("f_double"), 2.0, 10e-4);
        Assert.assertEquals(srt.min("f_int"), 0.0, 10e-4);
        Assert.assertEquals(srt.mean("f_double"), 0.3333333333333333, 10e-4);
        Assert.assertEquals(srt.variance("f_double"), 8.333333333333334, 10e-4);
        Assert.assertEquals(srt.standardDeviation("f_double"), 2.886751345948129, 10e-4);
        Assert.assertEquals(srt.normL1("f_double"), 7.0, 10e-4);
        Assert.assertEquals(srt.normL2("f_double"), 4.123105625617661, 10e-4);
    }


}