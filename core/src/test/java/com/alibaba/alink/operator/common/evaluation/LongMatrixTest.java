package com.alibaba.alink.operator.common.evaluation;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for LongMatrix.
 */
public class LongMatrixTest {
    private LongMatrix longMatrix = new LongMatrix(new long[][]{{5, 1, 2}, {1, 4, 0}});

    @Test
    public void defaultTest() {
        Assert.assertEquals(longMatrix.getRowNum(), 2);
        Assert.assertEquals(longMatrix.getColNum(), 3);
        Assert.assertArrayEquals(longMatrix.getRowSums(), new long[]{8L, 5L});
        Assert.assertArrayEquals(longMatrix.getColSums(), new long[]{6L, 5L, 2L});
        Assert.assertEquals(longMatrix.getTotal(), 13L);
        Assert.assertEquals(longMatrix.getValue(0, 1), 1L);
    }

    @Test
    public void plusEqual() {
        longMatrix.plusEqual(longMatrix);
        Assert.assertEquals(longMatrix.getTotal(), 26L);
    }

}