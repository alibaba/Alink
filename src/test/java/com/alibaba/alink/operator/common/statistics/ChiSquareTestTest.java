package com.alibaba.alink.operator.common.statistics;

import com.alibaba.alink.operator.common.statistics.ChiSquareTest;
import com.alibaba.alink.operator.common.statistics.Crosstab;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ChiSquareTestTest {

    @Test
    public void testChiSquare() {

        Crosstab crossTable = new Crosstab();

        crossTable.data = new long[][]{
            {4L, 1L, 3L},
            {2L, 4L, 5L},
            {3L, 4L, 4L}
        };

        Tuple4 tuple4 = ChiSquareTest.test(Tuple2.of(0, crossTable));

        assertEquals(0, tuple4.f0);
        assertEquals(1.0, (double) tuple4.f1, 10e-4);
        assertEquals(0.0, (double) tuple4.f2, 10e-4);

    }

    @Test
    public void testChiSqSelector() {
        String selectorType = "NumTopFeatures";
        int numTopFeatures = 2;
        double percentile = 0.5;
        double fpr = 0.5;
        double fdr = 0.5;
        double fwe = 0.5;

        int[] selectedIndices = testSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe);

        assertEquals(2, selectedIndices.length);
        assertEquals(0, selectedIndices[0]);
        assertEquals(2, selectedIndices[1]);
    }

    @Test
    public void testChiSqSelector2() {
        String selectorType = "percentile";
        int numTopFeatures = 2;
        double percentile = 0.5;
        double fpr = 0.5;
        double fdr = 0.5;
        double fwe = 0.5;

        int[] selectedIndices = testSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe);

        assertEquals(2, selectedIndices.length);
        assertEquals(0, selectedIndices[0]);
        assertEquals(2, selectedIndices[1]);
    }

    @Test
    public void testChiSqSelector3() {
        String selectorType = "fpr";
        int numTopFeatures = 2;
        double percentile = 0.5;
        double fpr = 0.5;
        double fdr = 0.5;
        double fwe = 0.5;

        int[] selectedIndices = testSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe);

        assertEquals(4, selectedIndices.length);
        assertEquals(0, selectedIndices[0]);
        assertEquals(1, selectedIndices[1]);
    }

    @Test
    public void testChiSqSelector4() {
        String selectorType = "fdr";
        int numTopFeatures = 2;
        double percentile = 0.5;
        double fpr = 0.5;
        double fdr = 0.5;
        double fwe = 0.5;

        int[] selectedIndices = testSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe);

        assertEquals(5, selectedIndices.length);
        assertEquals(0, selectedIndices[0]);
        assertEquals(1, selectedIndices[1]);
    }

    @Test
    public void testChiSqSelector5() {
        String selectorType = "fwe";
        int numTopFeatures = 2;
        double percentile = 0.5;
        double fpr = 0.5;
        double fdr = 0.5;
        double fwe = 0.5;

        int[] selectedIndices = testSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe);

        assertEquals(1, selectedIndices.length);
        assertEquals(0, selectedIndices[0]);
    }

    private int[] testSelector(String selectorType, int numTopFeatures,
                               double percentile,
                               double fpr,
                               double fdr,
                               double fwe) {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(0, 0.1, 1.0));
        data.add(Row.of(1, 0.3, 2.0));
        data.add(Row.of(2, 0.2, 4.0));
        data.add(Row.of(3, 0.4, 3.0));
        data.add(Row.of(4, 0.5, 4.0));

        return ChiSquareTest.selector(data, selectorType, numTopFeatures, percentile, fpr, fdr, fwe);
    }

}