package com.alibaba.alink.operator.common.clustering.lda;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OnlineCorpusStepTest {

    @Test
    public void calcTest() {
        List<Vector> data = new ArrayList<>();

        Row[] testArray =
            new Row[]{
                Row.of(new Object[]{0, "0:1 1:2 2:6 3:0 4:2 5:3 6:1 7:1 8:0 9:0 10:3"}),
                Row.of(new Object[]{1, "0:1 1:3 2:0 3:1 4:3 5:0 6:0 7:2 8:0 9:0 10:1"}),
                Row.of(new Object[]{2, "0:1 1:4 2:1 3:0 4:0 5:4 6:9 7:0 8:1 9:2 10:0"}),
                Row.of(new Object[]{3, "0:2 1:1 2:0 3:3 4:0 5:0 6:5 7:0 8:2 9:3 10:9"}),
                Row.of(new Object[]{4, "0:3 1:1 2:1 3:9 4:3 5:0 6:2 7:0 8:0 9:1 10:3"}),
                Row.of(new Object[]{5, "0:4 1:2 2:0 3:3 4:4 5:5 6:1 7:1 8:1 9:4 10:0"}),
                Row.of(new Object[]{6, "0:2 1:1 2:0 3:3 4:0 5:0 6:5 7:0 8:2 9:2 10:9"}),
                Row.of(new Object[]{7, "0:1 1:1 2:1 3:9 4:2 5:1 6:2 7:0 8:0 9:1 10:3"}),
                Row.of(new Object[]{8, "0:4 1:4 2:0 3:3 4:4 5:2 6:1 7:3 8:0 9:0 10:0"}),
                Row.of(new Object[]{9, "0:2 1:8 2:2 3:0 4:3 5:0 6:2 7:0 8:2 9:7 10:2"}),
                Row.of(new Object[]{10, "0:1 1:1 2:1 3:9 4:0 5:2 6:2 7:0 8:0 9:3 10:3"}),
                Row.of(new Object[]{11, "0:4 1:1 2:0 3:0 4:4 5:5 6:1 7:3 8:0 9:1 10:0"})
            };

        for (int i = 0; i < testArray.length; i++) {
            data.add(VectorUtil.parseSparse((String) testArray[i].getField(1)));
        }

        int row = 11;
        int col = 5;

        double[] temp = new double[]{0.8936825549031158,
            0.9650683744577933,
            1.1760851442955271,
            0.889011463028263,
            1.0355502890838704,
            1.1720254142865503,
            0.8496512959061578,
            1.1564109073902848,
            0.8528198328651976,
            1.072261907065107,
            1.0112487630821958,
            1.0288027427394206,
            1.1256918577237478,
            1.0641131417250107,
            0.9830788207753957,
            0.9519235842178695,
            1.0531103642783968,
            1.0846663792488604,
            0.9317316401779444,
            0.9816247167440154,
            0.953061129524052,
            0.8836097897537777,
            0.8539728772760822,
            1.109432137460693,
            0.9801693423689286,
            0.9385725168762017,
            1.009886079821316,
            0.9741390218380398,
            0.8734624459614093,
            0.8548583255850564,
            0.8934120594879987,
            1.0200469492393616,
            0.9461610896051537,
            1.1912819895664948,
            0.9650275833536232,
            0.9312815665885328,
            0.984681817963758,
            1.1412711858668625,
            1.1159082714127344,
            1.0219124026668207,
            1.1052645047308647,
            1.1380919062139254,
            0.9684793634316371,
            1.023922805813918,
            1.0777999541431174,
            0.8730213177341947,
            1.0353598060502658,
            1.047104264664753,
            1.1284793487722498,
            0.8898021261569816,
            1.1634869627283706,
            0.817874601150865,
            1.0424867867765728,
            1.167773175905418,
            0.915224402643435};

        DenseMatrix lambda = new DenseMatrix(row, col, temp, false).transpose();

        DenseMatrix alpha = new DenseMatrix(5, 1, new double[]{0.2, 0.3, 0.4, 0.5, 0.6});
        int vocabularySize = 11;
        int numTopic = 5;

        DenseMatrix gammad = new DenseMatrix(numTopic, 1, new double[]{0.7, 0.8, 0.9, 1.0, 1.1});

        Tuple4<DenseMatrix, DenseMatrix, Long, Long> tuple4 = OnlineCorpusStep.onlineCorpusUpdate(data, lambda,
            alpha,
            gammad,
            vocabularySize,
            numTopic);

        Assert.assertEquals(tuple4.f0.get(1, 0), 0.9862984592756618, 10e-4);
        Assert.assertEquals(tuple4.f1.get(1, 0), -73.2866169822335, 10e-4);
    }

}