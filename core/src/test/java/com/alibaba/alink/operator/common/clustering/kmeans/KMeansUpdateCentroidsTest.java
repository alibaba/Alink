package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceMatrixData;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.operator.common.clustering.kmeans.KMeansUpdateCentroids.updateCentroids;

public class KMeansUpdateCentroidsTest {

    @Test
    public void updateCentroidsTest(){
        int vectorSize = 2;
        int len = 5;
        EuclideanDistance distance = new EuclideanDistance();
        List<Row> list = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            Vector vec = DenseVector.ones(vectorSize).scale(i);
            list.add(Row.of(vec));
        }
        FastDistanceMatrixData matrixData = (FastDistanceMatrixData)
            distance.prepareMatrixData(list, 0).get(0);

        double[] buffer = new double[]{0.5, 0.5, 2.0, 2.5, 2.5, 2.0, 2.0, 2.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        int k = updateCentroids(matrixData, len, vectorSize, buffer, distance);
        Assert.assertEquals(k, 3);
        double[] expect = new double[]{0.25, 0.25, 1.25, 1.25, 2.0, 2.0, 0.0, 0.0, 0.0, 0.0};
        double[] predict = matrixData.getVectors().getData();
        for (int i = 0; i < expect.length; i++) {
            Assert.assertEquals(expect[i], predict[i], 0.01);
        }
    }
}
