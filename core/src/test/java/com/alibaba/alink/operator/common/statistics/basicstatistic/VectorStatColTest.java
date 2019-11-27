package com.alibaba.alink.operator.common.statistics.basicstatistic;

import org.junit.Test;

import static org.junit.Assert.*;

public class VectorStatColTest {

    @Test
    public void test() {
        VectorStatCol stat = new VectorStatCol();
        stat.visit(1.0);
        stat.visit(-2.0);
        stat.visit(Double.NaN);

        assertEquals(1.0, stat.max, 10e-6);
        assertEquals(-2.0, stat.min, 10e-6);
        assertEquals(5.0, stat.squareSum, 10e-6);
        assertEquals(3.0, stat.normL1, 10e-6);
        assertEquals(2.0, stat.numNonZero, 10e-6);
        assertEquals(-1.0, stat.sum, 10e-6);
        assertEquals(-0.5, stat.mean(2), 10e-6);
        assertEquals(4.5, stat.variance(2), 10e-6);
        assertEquals(2.12132, stat.standardDeviation(2), 10e-6);
    }

}