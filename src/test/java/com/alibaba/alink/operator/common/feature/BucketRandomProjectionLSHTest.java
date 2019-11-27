package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Test for HyperPlaneLSH.
 */
public class BucketRandomProjectionLSHTest {

    @Test
    public void testHashFunction() {
        BucketRandomProjectionLSH lsh = new BucketRandomProjectionLSH(0, 5, 2, 2, 1);
        Vector vec1 = new DenseVector(new double[]{1, 2, 3, 4, 5});
        Assert.assertEquals(new DenseVector(new double[]{-348137008, 1394862530}), lsh.hashFunction(vec1));
        Vector vec2 = new SparseVector(5, new int[]{0, 4}, new double[]{1.0, 4.0});
        Assert.assertEquals(new DenseVector(new double[]{-802232505, 1759100286}), lsh.hashFunction(vec2));
    }

    @Test
    public void testDistance() {
        BucketRandomProjectionLSH lsh = new BucketRandomProjectionLSH(0, 5, 2, 2, 1);
        Vector vec1 = new DenseVector(new double[]{1, 0, 0, 2, 0});
        Vector vec2 = new DenseVector(new double[]{0, 1, 0, 2, 1});
        Assert.assertEquals(1.732, lsh.keyDistance(vec1, vec2), 0.001);

        vec1 = new SparseVector(10, new int[]{0, 4, 5, 7, 9}, new double[]{1.0, 1.0, 1.0, 1.0, 1.0});
        vec2 = new SparseVector(10, new int[]{0, 1, 3, 5, 9}, new double[]{1.0, 1.0, 1.0, 1.0, 1.0});
        Assert.assertEquals(2.0, lsh.keyDistance(vec1, vec2), 0.001);
    }
}