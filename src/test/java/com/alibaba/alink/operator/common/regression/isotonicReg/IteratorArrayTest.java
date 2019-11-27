package com.alibaba.alink.operator.common.regression.isotonicReg;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test of IteratorArray.
 */

public class IteratorArrayTest {

    @Test
    public void iteratorArrayTest() {
        IteratorArray iteratorArray = new IteratorArray(10, 1);
        Assert.assertEquals(iteratorArray.getPoint(), 1);
        Assert.assertTrue(iteratorArray.hasPrevious());
        Assert.assertTrue(iteratorArray.hasNext());
        iteratorArray.retreat();
        Assert.assertFalse(iteratorArray.hasPrevious());
        Assert.assertEquals(iteratorArray.getPoint(), 0);
        iteratorArray = new IteratorArray(10, 9);
        Assert.assertFalse(iteratorArray.hasNext());
    }

}