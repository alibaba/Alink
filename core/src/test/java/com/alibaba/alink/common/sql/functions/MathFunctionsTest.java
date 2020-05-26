package com.alibaba.alink.common.sql.functions;

import com.alibaba.alink.operator.common.sql.functions.MathFunctions;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

public class MathFunctionsTest {

    private static final double EPS = 1e-9;

    @Test
    public void testLog2() throws InvocationTargetException, IllegalAccessException {
        double v = 10;
        double base = 2;
        double expected = Math.log(v) / Math.log(base);
        Assert.assertEquals(expected, (Double) MathFunctions.LOG2.invoke(null, v), EPS);
        Assert.assertEquals(expected, (Double) MathFunctions.LOG2_DEC.invoke(null, BigDecimal.valueOf(v)), EPS);
    }

    @Test
    public void testLog() throws InvocationTargetException, IllegalAccessException {
        double v = 10;
        double expected = Math.log(v);
        Assert.assertEquals(expected, (Double) MathFunctions.LOG.invoke(null, v), EPS);
        Assert.assertEquals(expected, (Double) MathFunctions.LOG_DEC.invoke(null, BigDecimal.valueOf(v)), EPS);
    }

    @Test
    public void testLogWithBase() throws InvocationTargetException, IllegalAccessException {
        double v = 10;
        double base = 3;
        double expected = Math.log(v) / Math.log(base);
        Assert.assertEquals(expected, (Double) MathFunctions.LOG_WITH_BASE.invoke(null, base, v), EPS);
        Assert.assertEquals(expected, (Double) MathFunctions.LOG_WITH_BASE_DEC_DOU.invoke(null, BigDecimal.valueOf(base), v), EPS);
        Assert.assertEquals(expected, (Double) MathFunctions.LOG_WITH_BASE_DOU_DEC.invoke(null, base, BigDecimal.valueOf(v)), EPS);
        Assert.assertEquals(expected, (Double) MathFunctions.LOG_WITH_BASE_DEC_DEC.invoke(null, BigDecimal.valueOf(base), BigDecimal.valueOf(v)), EPS);
    }

    @Test
    public void testSinh() throws InvocationTargetException, IllegalAccessException {
        double v = 10;
        double expected = Math.sinh(v);
        Assert.assertEquals(expected, (Double) MathFunctions.SINH.invoke(null, v), EPS);
        Assert.assertEquals(expected, (Double) MathFunctions.SINH_DEC.invoke(null, BigDecimal.valueOf(v)), EPS);
    }

    @Test
    public void testCosh() throws InvocationTargetException, IllegalAccessException {
        double v = 10;
        double expected = Math.cosh(v);
        Assert.assertEquals(expected, (Double) MathFunctions.COSH.invoke(null, v), EPS);
        Assert.assertEquals(expected, (Double) MathFunctions.COSH_DEC.invoke(null, BigDecimal.valueOf(v)), EPS);
    }

    @Test
    public void testTanh() throws InvocationTargetException, IllegalAccessException {
        double v = 10;
        double expected = Math.tanh(v);
        Assert.assertEquals(expected, (Double) MathFunctions.TANH.invoke(null, v), EPS);
        Assert.assertEquals(expected, (Double) MathFunctions.TANH_DEC.invoke(null, BigDecimal.valueOf(v)), EPS);
    }

    @Test
    public void testUuid() throws InvocationTargetException, IllegalAccessException {
        String actual = (String) MathFunctions.UUID.invoke(null);
        Assert.assertTrue(actual.matches("[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"));
    }

    @Test
    public void testBin() throws InvocationTargetException, IllegalAccessException {
        long v = 100;
        String expected = Long.toBinaryString(v);
        System.out.println(expected);
        Assert.assertEquals(expected, MathFunctions.BIN.invoke(null, v));
    }

    @Test
    public void testHex() throws InvocationTargetException, IllegalAccessException {
        long v = 100;
        String expected = Long.toHexString(v);
        Assert.assertEquals(expected, MathFunctions.HEX_LONG.invoke(null, v));
    }

    @Test
    public void testHexString() throws InvocationTargetException, IllegalAccessException {
        String v = "hello,world";
        String expected = "68656C6C6F2C776F726C64";
        Assert.assertEquals(expected, MathFunctions.HEX_STRING.invoke(null, v));
    }
}
