package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import com.alibaba.alink.params.feature.BasedChisqSelectorParams;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Chisq selector util.
 */
public class ChisqSelectorUtil {

    /**
     * chi-square selector for table data.
     *
     * @param chiSquareTest:  first entry is colIdx, second entry is chi-square test.
     * @param selectorType:   "NumTopFeatures", "percentile", "fpr", "fdr", "fwe"
     * @param numTopFeatures: if selectorType is NumTopFeatures, select the largest NumTopFeatures features.
     * @param percentile:     if selectorType is percentile, select the largest percentile * numFeatures features.
     * @param fpr:            if selectorType is fpr, select feature which chi-square value less than fpr.
     * @param fdr:            if selectorType is fdr, select feature which chi-square value less than fdr * (i + 1) / n.
     * @param fwe:            if selectorType is fwe, select feature which chi-square value less than fwe / n.
     * @return selected col indices.
     */
    public static int[] selector(List<ChiSquareTestResult> chiSquareTest,
                                 BasedChisqSelectorParams.SelectorType selectorType,
                                 int numTopFeatures,
                                 double percentile,
                                 double fpr,
                                 double fdr,
                                 double fwe) {


        int len = chiSquareTest.size();

        List<Integer> selectedColIndices = new ArrayList<>();
        switch (selectorType) {
            case NumTopFeatures:
                chiSquareTest.sort(new RowAscComparator(false, true));

                for (int i = 0; i < numTopFeatures && i < len; i++) {
                    selectedColIndices.add(getIdx(chiSquareTest.get(i)));
                }

                break;
            case PERCENTILE:
                chiSquareTest.sort(new RowAscComparator(false, true));
                int size = (int) (len * percentile);
                if (size == 0) {
                    size = 1;
                }
                for (int i = 0; i < size && i < len; i++) {
                    selectedColIndices.add(getIdx(chiSquareTest.get(i)));
                }
                break;
            case FPR:
                for (ChiSquareTestResult row : chiSquareTest) {
                    if (row.getValue() < fpr) {
                        selectedColIndices.add(getIdx(row));
                    }
                }
                break;
            case FDR:
                chiSquareTest.sort(new RowAscComparator(false, true));
                int maxIdx = 0;
                for (int i = 0; i < len; i++) {
                    ChiSquareTestResult row = chiSquareTest.get(i);
                    if (row.getValue() <= fdr * (i + 1) / len) {
                        maxIdx = i;
                    }
                }

                for (int i = 0; i <= maxIdx; i++) {
                    selectedColIndices.add(getIdx(chiSquareTest.get(i)));
                }
                Collections.sort(selectedColIndices);
                break;
            case FWE:
                for (ChiSquareTestResult row : chiSquareTest) {
                    if (row.getValue() <= fwe / len) {
                        selectedColIndices.add(getIdx(row));
                    }
                }
                break;
        }

        return Ints.toArray(selectedColIndices);
    }


    /**
     * chi-square selector and build model.
     */
    public static class ChiSquareSelector implements MapPartitionFunction<Row, Row> {
        private static final long serialVersionUID = -482962272562482883L;
        private String[] cols;
        private BasedChisqSelectorParams.SelectorType selectorType;
        private int numTopFeatures;
        private double percentile;
        private double fpr;
        private double fdr;
        private double fwe;

        public ChiSquareSelector(String[] cols,
                                 BasedChisqSelectorParams.SelectorType selectorType, int numTopFeatures,
                                 double percentile, double fpr,
                                 double fdr, double fwe) {
            this.cols = cols;
            this.selectorType = selectorType;
            this.numTopFeatures = numTopFeatures;
            this.percentile = percentile;
            this.fpr = fpr;
            this.fdr = fdr;
            this.fwe = fwe;
        }

        @Override
        public void mapPartition(Iterable<Row> iterable, Collector<Row> collector) {
            List<ChiSquareTestResult> chiSquareTest = new ArrayList<>();
            for (Row row : iterable) {
                //f0: id, f1:p, f2: chisq, f3: df
                chiSquareTest.add(new ChiSquareTestResult(
                    (double) row.getField(3),
                    (double) row.getField(1),
                    (double) row.getField(2),
                    row.getField(0).toString()));
            }

            int[] selectedIndices = selector(chiSquareTest,
                selectorType,
                numTopFeatures,
                percentile,
                fpr,
                fdr,
                fwe);

            ChisqSelectorModelInfo modelInfo = new ChisqSelectorModelInfo();
            modelInfo.chiSqs = chiSquareTest.toArray(new ChiSquareTestResult[0]);
            modelInfo.colNames = cols;
            modelInfo.fwe = fwe;
            modelInfo.fdr = fdr;
            modelInfo.fpr = fpr;
            modelInfo.percentile = percentile;
            modelInfo.numTopFeatures = numTopFeatures;
            modelInfo.selectorType = selectorType;

            modelInfo.siftOutColNames = new String[selectedIndices.length];

            for (int i = 0; i < selectedIndices.length; i++) {
                modelInfo.siftOutColNames[i] =
                    cols == null ? String.valueOf(selectedIndices[i]) : cols[selectedIndices[i]];
            }
            if (cols != null) {
                for (int i = 0; i < modelInfo.chiSqs.length; i++) {
                    modelInfo.chiSqs[i].setColName(cols[getIdx(modelInfo.chiSqs[i])]);
                }
            }

            new ChiSqSelectorModelDataConverter().save(modelInfo, collector);
        }
    }

    /**
     * row asc comparator.
     */
    static class RowAscComparator implements Comparator<ChiSquareTestResult> {
        private boolean isChisq;
        private boolean isDes;

        public RowAscComparator(boolean isChisq, boolean isDes) {
            this.isChisq = isChisq;
            this.isDes = isDes;
        }

        @Override
        public int compare(ChiSquareTestResult o1, ChiSquareTestResult o2) {
            double d1;
            double d2;
            if (isChisq) {
                d1 = o1.getValue();
                d2 = o2.getValue();
            } else {
                d1 = o1.getP();
                d2 = o2.getP();
            }

            return isDes ? Double.compare(d1, d2) : -Double.compare(d1, d2);
        }
    }

    /**
     * find index.
     */
    static int getIdx(ChiSquareTestResult test) {
        return (int) Math.round(Double.parseDouble(test.getColName()));
    }


}