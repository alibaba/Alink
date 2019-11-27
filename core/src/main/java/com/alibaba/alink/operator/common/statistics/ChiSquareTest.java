package com.alibaba.alink.operator.common.statistics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.common.feature.ChiSqSelectorModelDataConverter;
import com.google.common.primitives.Ints;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * for chi-square test and chi-square selector.
 */
public class ChiSquareTest {

    /**
     * @param in: the last col is label col, others are selectedCols.
     * @param sessionId: sessionId
     * @return 3 cols, 1th is colId, 2th is pValue, 3th is chi-square value
     *
     */
    protected static DataSet<Row> test(DataSet<Row> in, Long sessionId) {
        //flatting data to triple.
        DataSet<Row> dataSet = in
            .flatMap(new FlatMapFunction<Row, Row>() {
                @Override
                public void flatMap(Row row, Collector<Row> result) {
                    int n = row.getArity() - 1;
                    String nStr = String.valueOf(row.getField(n));
                    for (int i = 0; i < n; i++) {
                        Row out = new Row(3);
                        out.setField(0, i);
                        out.setField(1, String.valueOf(row.getField(i)));
                        out.setField(2, nStr);
                        result.collect(out);
                    }
                }
            });

        Table data = DataSetConversionUtil.toTable(
            sessionId,
            dataSet,
            new String[]{"col", "feature", "label"},
            new TypeInformation[]{Types.INT, Types.STRING, Types.STRING});


        //calculate cross table  and chiSquare test.
        return DataSetConversionUtil.fromTable(sessionId, data
            .groupBy("col,feature,label")
            .select("col,feature,label,count(1) as count2"))
            .groupBy("col").reduceGroup(
                new GroupReduceFunction<Row, Tuple2<Integer, Crosstab>>() {
                    @Override
                    public void reduce(Iterable<Row> iterable, Collector<Tuple2<Integer, Crosstab>> collector) {
                        Map<Tuple2<String, String>, Long> map = new HashMap<>();
                        int colIdx = -1;
                        for (Row row : iterable) {
                            map.put(Tuple2.of(row.getField(1).toString(),
                                    row.getField(2).toString()),
                                (long) row.getField(3));
                            colIdx = (Integer) row.getField(0);
                        }
                        collector.collect(new Tuple2<>(colIdx, Crosstab.convert(map)));
                    }
                })
            .map(new ChiSquareTestFromCrossTable());
    }

    /**
     * chi-square selector for table data.
     * @param chiSquareTest: first entry is colIdx, second entry is chi-square test.
     * @param selectorType: "numTopFeatures", "percentile", "fpr", "fdr", "fwe"
     * @param numTopFeatures: if selectorType is numTopFeatures, select the largest numTopFeatures features.
     * @param percentile: if selectorType is percentile, select the largest percentile * numFeatures features.
     * @param fpr: if selectorType is fpr, select feature which chi-square value less than fpr.
     * @param fdr: if selectorType is fdr, select feature which chi-square value less than fdr * (i + 1) / n.
     * @param fwe: if selectorType is fwe, select feature which chi-square value less than fwe / n.
     * @return selected col indices.
     */
    protected static int[] selector(List<Row> chiSquareTest,
                                    String selectorType,
                                    int numTopFeatures,
                                    double percentile,
                                    double fpr,
                                    double fdr,
                                    double fwe) {


        int len = chiSquareTest.size();

        if(selectorType.toUpperCase().equals("NUMTOPFEATURES")) {
            selectorType = "NUM_TOP_FEATURES";
        }
        ChiSqSelectorType type = ChiSqSelectorType.valueOf(selectorType.toUpperCase());

        List<Integer> selectedColIndices = new ArrayList<>();
        switch (type) {
            case NUM_TOP_FEATURES:
                chiSquareTest.sort(new RowAscComparator());

                for (int i = 0; i < numTopFeatures && i < len; i++) {
                    selectedColIndices.add((int) chiSquareTest.get(i).getField(0));
                }

                break;
            case PERCENTILE:
                chiSquareTest.sort(new RowAscComparator());
                int size = (int) (len * percentile);
                if (size == 0) {
                    size = 1;
                }
                for (int i = 0; i < size && i < len; i++) {
                    selectedColIndices.add((int) chiSquareTest.get(i).getField(0));
                }
                break;
            case FPR:
                for (Row row : chiSquareTest) {
                    if ((double) row.getField(1) < fpr) {
                        selectedColIndices.add((int) row.getField(0));
                    }
                }
                break;
            case FDR:
                chiSquareTest.sort(new RowAscComparator());
                int maxIdx = 0;
                for (int i = 0; i < len; i++) {
                    Row row = chiSquareTest.get(i);
                    if ((double) row.getField(1) <= fdr * (i + 1) / len) {
                        maxIdx = i;
                    }
                }

                for (int i = 0; i <= maxIdx; i++) {
                    selectedColIndices.add((int) chiSquareTest.get(i).getField(0));
                }
                Collections.sort(selectedColIndices);
                break;
            case FWE:
                for (Row row : chiSquareTest) {
                    if ((double) row.getField(1) <= fwe / len) {
                        selectedColIndices.add((int) row.getField(0));
                    }
                }
                break;
            default:
                throw new RuntimeException("Selector Type not support. " + selectorType);
        }

        return Ints.toArray(selectedColIndices);
    }


    /**
     * @param crossTabWithId: f0 is id, f1 is cross table
     * @return tuple4: f0 is id which is id of cross table, f1 is pValue, f2 is chi-square Value, f3 is df
     */
    protected static Tuple4<Integer, Double, Double, Double> test(Tuple2<Integer, Crosstab> crossTabWithId) {
        int colIdx = crossTabWithId.f0;
        Crosstab crosstab = crossTabWithId.f1;

        int rowLen = crosstab.rowTags.size();
        int colLen = crosstab.colTags.size();

        //compute row sum and col sum
        double[] rowSum = crosstab.rowSum();
        double[] colSum = crosstab.colSum();
        double n = crosstab.sum();


        //compute statistic value
        double chiSq = 0;
        for (int i = 0; i < rowLen; i++) {
            for (int j = 0; j < colLen; j++) {
                double nij = rowSum[i] * colSum[j] / n;
                double temp = crosstab.data[i][j] - nij;
                chiSq += temp * temp / nij;
            }
        }

        //set result
        double p;
        if (rowLen <= 1 || colLen <= 1) {
            p = 1;
        } else {
            ChiSquaredDistribution distribution =
                new ChiSquaredDistribution(null, (rowLen - 1) * (colLen - 1));
            p = 1.0 - distribution.cumulativeProbability(Math.abs(chiSq));
        }

        return Tuple4.of(colIdx, p, chiSq, (double)(rowLen - 1) * (colLen - 1));
    }

    /**
     * chi-square selector and build model.
     */
    protected static class ChiSquareSelector implements MapPartitionFunction<Row, Row> {
        private String selectorType;
        private int numTopFeatures;
        private double percentile;
        private double fpr;
        private double fdr;
        private double fwe;

        ChiSquareSelector(String selectorType, int numTopFeatures,
                          double percentile, double fpr,
                          double fdr, double fwe) {
            this.selectorType = selectorType;
            this.numTopFeatures = numTopFeatures;
            this.percentile = percentile;
            this.fpr = fpr;
            this.fdr = fdr;
            this.fwe = fwe;
        }

        @Override
        public void mapPartition(Iterable<Row> iterable, Collector<Row> collector) {
            List<Row> chiSquareTest = new ArrayList<>();
            for (Row row : iterable) {
                chiSquareTest.add(row);
            }

            int[] selectedIndices = selector(chiSquareTest,
                selectorType,
                numTopFeatures,
                percentile,
                fpr,
                fdr,
                fwe);

            new ChiSqSelectorModelDataConverter().save(selectedIndices, collector);
        }
    }

    /**
     * row asc comparator.
     */
    static class RowAscComparator implements Comparator<Row> {
        @Override
        public int compare(Row o1, Row o2) {
            double d1 = (double) o1.getField(1);
            double d2 = (double) o2.getField(1);

            return Double.compare(d1, d2);
        }
    }

    /**
     * calculate chi-square test value from cross table.
     */
    public static class ChiSquareTestFromCrossTable implements MapFunction<Tuple2<Integer, Crosstab>, Row> {

        ChiSquareTestFromCrossTable() {
        }

        /**
         * The mapping method. Takes an element from the input data set and transforms
         * it into exactly one element.
         *
         * @param crossTabWithId The input value.
         * @return The transformed value
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */
        @Override
        public Row map(Tuple2<Integer, Crosstab> crossTabWithId) throws Exception {
            Tuple4 tuple4 = test(crossTabWithId);

            Row row = new Row(4);
            row.setField(0, tuple4.f0);
            row.setField(1, tuple4.f1);
            row.setField(2, tuple4.f2);
            row.setField(3, tuple4.f3);

            return row;
        }
    }

    /**
     * chi-square selector type.
     */

    public enum ChiSqSelectorType {
        /**
         * select numTopFeatures features which are maximum chi-square value.
         */
        NUM_TOP_FEATURES,

        /**
         * select percentile * n features which are maximum chi-square value.
         */
        PERCENTILE,

        /**
         * select feature which chi-square value less than fpr.
         */
        FPR,

        /**
         * select feature which chi-square value less than fdr * (i + 1) / n.
         */
        FDR,

        /**
         * select feature which chi-square value less than fwe / n.
         */
        FWE
    }
}
