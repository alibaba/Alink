package com.alibaba.alink.operator.common.feature.binning;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for FeatureBinsUtil.
 */
public class FeatureBinsUtilTest {

    @Test
    public void keepGivenDecimal() {
        double d = 1.5678;
        Assert.assertEquals(FeatureBinsUtil.keepGivenDecimal(d, 0).toString(), "2.0");
    }

    @Test
    public void keepGivenDecimalNumber() {
        Number d = 1.5678;
        Assert.assertEquals(FeatureBinsUtil.keepGivenDecimal(d, 3).toString(), "1.568");
        d = 233421L;
        Assert.assertEquals(FeatureBinsUtil.keepGivenDecimal(d, 3).toString(), "233421");
    }

    @Test
    public void testWoe(){
        long positive = 2500;
        long negative = 47500;
        long total = 100000;
        long positiveTotal = 10000;

        Assert.assertEquals(FeatureBinsUtil.calcWoe(positive + negative, positive, positiveTotal, total - positiveTotal), -0.74, 0.01);
    }
}