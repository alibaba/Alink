package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.common.linalg.DenseVector;
import org.junit.Assert;
import org.junit.Test;


public class DenseVectorSummaryTest {

    @Test
    public void test() {
        DenseVectorSummary srt = summary();

        Assert.assertEquals(3, srt.vectorSize());
        Assert.assertEquals(5, srt.count());
        Assert.assertEquals(-1.0, srt.max(1), 10e-4);
        Assert.assertEquals(-5.0, srt.min(1), 10e-4);
        Assert.assertEquals(-15.0, srt.sum(1), 10e-4);
        Assert.assertEquals(-3.0, srt.mean(1), 10e-4);
        Assert.assertEquals(2.5, srt.variance(1), 10e-4);
        Assert.assertEquals(1.5811, srt.standardDeviation(1), 10e-4);
        Assert.assertEquals(15.0, srt.normL1(1), 10e-4);
        Assert.assertEquals(7.416198, srt.normL2(1), 10e-4);

        Assert.assertArrayEquals(new double[]{5.0, -1.0, 3.0}, ((DenseVector)srt.max()).getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{1.0, -5.0, 3.0}, ((DenseVector)srt.min()).getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{15.0, -15.0, 15.0}, ((DenseVector)srt.sum()).getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{3.0, -3.0, 3.0}, ((DenseVector)srt.mean()).getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{2.5, 2.5, 0.0}, ((DenseVector)srt.variance()).getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{1.5811, 1.5811, 0.0}, ((DenseVector)srt.standardDeviation()).getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{15, 15, 15}, ((DenseVector)srt.normL1()).getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{7.416198, 7.416198, 6.7082}, ((DenseVector)srt.normL2()).getData(), 10e-4);
    }

    private DenseVectorSummary summary() {
        DenseVector[] data =
            new DenseVector[]{
                new DenseVector(new double[]{1.0, -1.0, 3.0}),
                new DenseVector(new double[]{2.0, -2.0, 3.0}),
                new DenseVector(new double[]{3.0, -3.0, 3.0}),
                new DenseVector(new double[]{4.0, -4.0, 3.0}),
                new DenseVector(new double[]{5.0, -5.0, 3.0})
            };

        DenseVectorSummarizer summarizer = new DenseVectorSummarizer();
        for (DenseVector aData : data) {
            summarizer.visit(aData);
        }
        return (DenseVectorSummary) summarizer.toSummary();
    }

}