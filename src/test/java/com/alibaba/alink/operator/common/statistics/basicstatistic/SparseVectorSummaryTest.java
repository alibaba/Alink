package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SparseVectorSummaryTest {
    @Test
    public void test() {
        SparseVectorSummary srt = summarizer();

        assertEquals(5, srt.colNum);
        assertEquals(5, srt.vectorSize());

        Assert.assertEquals(2.0, srt.max(1), 10e-4);
        Assert.assertEquals(-5.0, srt.min(1), 10e-4);
        Assert.assertEquals(-4.0, srt.sum(1), 10e-4);
        Assert.assertEquals(-0.8, srt.mean(1), 10e-4);
        Assert.assertEquals(6.7, srt.variance(1), 10e-4);
        Assert.assertEquals(2.588436, srt.standardDeviation(1), 10e-4);
        Assert.assertEquals(8.0, srt.normL1(1), 10e-4);
        Assert.assertEquals(5.477226, srt.normL2(1), 10e-4);
        Assert.assertEquals(3, srt.numNonZero(1), 10e-4);

        Assert.assertArrayEquals(new double[]{3.0, 3.0, 4.0, 3.0, 2.0}, ((DenseVector) srt.numNonZero()).getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{5.0, 2.0, 3.0, 3.0, 3.0}, ((SparseVector) srt.max()).toDenseVector().getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{0.0, -5.0, -4.0, -3.0, 0.0}, ((SparseVector) srt.min()).toDenseVector().getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{10.0, -4.0, 0.0, 3.0, 6.0}, ((SparseVector) srt.sum()).toDenseVector().getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{2.0, -0.8, 0.0, 0.6, 1.2}, ((SparseVector) srt.mean()).toDenseVector().getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{5.5, 6.7, 9.5, 6.3, 2.7}, ((SparseVector) srt.variance()).toDenseVector().getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{2.345208, 2.588436, 3.082207, 2.509980, 1.643168}, ((SparseVector) srt.standardDeviation()).toDenseVector().getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{10, 8.0, 12.0, 9.0, 6.0}, ((SparseVector) srt.normL1()).toDenseVector().getData(), 10e-4);
        Assert.assertArrayEquals(new double[]{6.480741, 5.477226, 6.164414, 5.196152, 4.242641}, ((SparseVector) srt.normL2()).toDenseVector().getData(), 10e-4);
    }


    private SparseVectorSummary summarizer() {
        SparseVector[] data =
            new SparseVector[]{
                new SparseVector(5, new int[]{0, 1, 2}, new double[]{1.0, -1.0, 3.0}),
                new SparseVector(5, new int[]{1, 2, 3}, new double[]{2.0, -2.0, 3.0}),
                new SparseVector(5, new int[]{2, 3, 4}, new double[]{3.0, -3.0, 3.0}),
                new SparseVector(5, new int[]{0, 2, 3}, new double[]{4.0, -4.0, 3.0}),
                new SparseVector(5, new int[]{0, 1, 4}, new double[]{5.0, -5.0, 3.0})
            };

        SparseVectorSummarizer summarizer = new SparseVectorSummarizer();
        for (SparseVector aData : data) {
            summarizer.visit(aData);
        }
        return (SparseVectorSummary) summarizer.toSummary();
    }

}