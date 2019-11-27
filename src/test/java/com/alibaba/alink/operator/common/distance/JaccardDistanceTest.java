package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for JaccardDistance.
 */
public class JaccardDistanceTest {

    @Test
    public void testContinuousDistance() {
        DenseVector vec1 = new DenseVector(new double[] {1, 0, 4, 0, 3});
        DenseVector vec2 = new DenseVector(new double[] {0, 6, 1, 0, 4});
        SparseVector vec3 = new SparseVector(5, new int[] {1, 3}, new double[] {0.1, 0.4});
        SparseVector vec4 = new SparseVector(5, new int[] {2, 3}, new double[] {0.4, 0.1});

        ContinuousDistance distance = new JaccardDistance();
        Assert.assertEquals(distance.calc(vec1, vec2), 0.5, 0.01);
        Assert.assertEquals(distance.calc(vec1.getData(), vec2.getData()), 0.5, 0.01);
        Assert.assertEquals(distance.calc(vec1, vec3), 1.0, 0.01);
        Assert.assertEquals(distance.calc(vec3, vec4), 0.66, 0.01);
        Assert.assertEquals(distance.calc(vec3, vec1), 1.0, 0.01);
    }
}