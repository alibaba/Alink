package com.alibaba.alink.operator.common.statistics;

import com.alibaba.alink.operator.common.feature.ChisqSelectorUtil;
import com.alibaba.alink.params.feature.BasedChisqSelectorParams;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;
import org.junit.Assert;
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
        BasedChisqSelectorParams.SelectorType selectorType = BasedChisqSelectorParams.SelectorType.NumTopFeatures;
        int numTopFeatures = 2;
        double percentile = 0.5;
        double fpr = 0.5;
        double fdr = 0.5;
        double fwe = 0.5;

        int[] selectedIndices = testSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe);

        assertEquals(2, selectedIndices.length);
        assertEquals(0, selectedIndices[0]);
        assertEquals(1, selectedIndices[1]);
    }

    @Test
    public void testChiSqSelector2() {
        BasedChisqSelectorParams.SelectorType selectorType = BasedChisqSelectorParams.SelectorType.PERCENTILE;
        int numTopFeatures = 2;
        double percentile = 0.5;
        double fpr = 0.5;
        double fdr = 0.5;
        double fwe = 0.5;

        int[] selectedIndices = testSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe);

        assertEquals(2, selectedIndices.length);
        assertEquals(0, selectedIndices[0]);
        assertEquals(1, selectedIndices[1]);
    }

    @Test
    public void testChiSqSelector3() {
        BasedChisqSelectorParams.SelectorType selectorType = BasedChisqSelectorParams.SelectorType.FPR;
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
        BasedChisqSelectorParams.SelectorType selectorType = BasedChisqSelectorParams.SelectorType.FDR;
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
        BasedChisqSelectorParams.SelectorType selectorType = BasedChisqSelectorParams.SelectorType.FWE;
        int numTopFeatures = 2;
        double percentile = 0.5;
        double fpr = 0.5;
        double fdr = 0.5;
        double fwe = 0.5;

        int[] selectedIndices = testSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe);

        assertEquals(1, selectedIndices.length);
        assertEquals(0, selectedIndices[0]);
    }

    @Test
    public void testChisqSelectorMap() {
        ChisqSelectorUtil.ChiSquareSelector selector =
            new ChisqSelectorUtil.ChiSquareSelector(null, BasedChisqSelectorParams.SelectorType.NumTopFeatures,
                5, 0, 0, 0, 0);

        List<Row> rowList = new ArrayList<>();
        ListCollector<Row> rows = new ListCollector<Row>(rowList);

        List<Row> test = new ArrayList<>();
        test.add(Row.of("1", 0.1, 0.1, 1.0));
        test.add(Row.of("2", 0.2, 0.2, 2.0));
        test.add(Row.of("3", 0.3, 0.3, 3.0));
        test.add(Row.of("4", 0.4, 0.4, 4.0));

        selector.mapPartition(test, rows);

        for(Row row: rowList) {
            if((long)row.getField(0) == 1048576) {
                Assert.assertEquals("{\"chiSqs\":[{\"colName\":\"1\",\"df\":1.0,\"p\":0.1,\"value\":0.1},{\"colName\":\"2\",\"df\":2.0,\"p\":0.2,\"value\":0.2},{\"colName\":\"3\",\"df\":3.0,\"p\":0.3,\"value\":0.3},{\"colName\":\"4\",\"df\":4.0,\"p\":0.4,\"value\":0.4}],\"colNames\":null,\"siftOutColNames\":[\"1\",\"2\",\"3\",\"4\"],\"selectorType\":\"NumTopFeatures\",\"numTopFeatures\":5,\"percentile\":0.0,\"fpr\":0.0,\"fdr\":0.0,\"fwe\":0.0}"
                    , (String)row.getField(1));
            }
        }
    }

    private int[] testSelector(BasedChisqSelectorParams.SelectorType selectorType, int numTopFeatures,
                               double percentile,
                               double fpr,
                               double fdr,
                               double fwe) {
        List<ChiSquareTestResult> data = new ArrayList<>();
        data.add(new ChiSquareTestResult(0, 1.0, 0.1, "0"));
        data.add(new ChiSquareTestResult(1, 2.0, 0.3, "1"));
        data.add(new ChiSquareTestResult(2, 4.0, 0.2, "2"));
        data.add(new ChiSquareTestResult(3, 3.0, 0.4, "3"));
        data.add(new ChiSquareTestResult(4, 4.0, 0.5, "4"));

        return ChisqSelectorUtil.selector(data, selectorType, numTopFeatures, percentile, fpr, fdr, fwe);
    }

}