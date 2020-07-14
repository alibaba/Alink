package com.alibaba.alink.common.lazy;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class LazyEvaluationTest extends BaseLazyTest {

    @Test
    public void testMultipleValuesAndCallbacks() {
        LazyEvaluation<Long> lazyLong = new LazyEvaluation<>();
        lazyLong.addCallback(d -> System.out.println(d - 1));
        lazyLong.addValue(100L);
        lazyLong.addValue(300L);
        lazyLong.addValue(400L);
        lazyLong.addCallback(d -> System.out.println(d + 1));
        lazyLong.addValue(200L);
        lazyLong.addCallback(d -> System.out.println(d + 2));
        Assert.assertEquals(Long.valueOf(200L), lazyLong.getLatestValue());
        // should print (4 values * 3 callbacks =) 12 lines
        Assert.assertEquals(12, StringUtils.countMatches(outContent.toString(), "\n"));
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionInCallback() {
        LazyEvaluation<Long> lazyLong = new LazyEvaluation<>();
        lazyLong.addCallback(d -> {
            Assert.fail();
        });
        lazyLong.addValue(100L);
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionNoValue() {
        LazyEvaluation<Long> lazyLong = new LazyEvaluation<>();
        lazyLong.getLatestValue();
        System.out.println("latest value: " + lazyLong.getLatestValue());
    }
}
