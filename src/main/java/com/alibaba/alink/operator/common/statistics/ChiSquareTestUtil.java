package com.alibaba.alink.operator.common.statistics;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.ChiSqSelectorModelDataConverter;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

/**
 * for vector chi-square test and chi-square test.
 */
public class ChiSquareTestUtil {

    /**
     * chi-square test for vector data.
     * @param in: input
     * @param vectorColName: vector col name
     * @param labelColName: label col name
     * @return chi-square test result
     */
    public static DataSet<Row> vectorTest(BatchOperator in,
                                          String vectorColName,
                                          String labelColName) {
        DataSet<Row> dataSet = StatisticsHelper.transformToColumns(in, null,
            vectorColName, new String[]{labelColName});

        return ChiSquareTest.test(dataSet, in.getMLEnvironmentId());
    }


    /**
     * chi-square test for table data.
     * @param in: input
     * @param selectedColNames: selected col names
     * @param labelColName: label col name
     * @return chi-square test result
     */
    public static DataSet<Row> test(BatchOperator in,
                                    String[] selectedColNames,
                                    String labelColName) {
        in = in.select(ArrayUtils.add(selectedColNames, labelColName));
        return ChiSquareTest.test(in.getDataSet(), in.getMLEnvironmentId());
    }

    /**
     * chi-square selector for table data.
     * @param in: input
     * @param selectedColNames: selected col names
     * @param labelColName: label col name
     * @param selectorType: "numTopFeatures", "percentile", "fpr", "fdr", "fwe"
     * @param numTopFeatures: if selectorType is numTopFeatures, select the largest numTopFeatures features.
     * @param percentile: if selectorType is percentile, select the largest percentile * numFeatures features.
     * @param fpr: if selectorType is fpr, select feature which chi-square value less than fpr.
     * @param fdr: if selectorType is fdr, select feature which chi-square value less than fdr * (i + 1) / n.
     * @param fwe: if selectorType is fwe, select feature which chi-square value less than fwe / n.
     * @return selected col indices.
     */
    public static Table selector(BatchOperator in,
                                 String[] selectedColNames,
                                 String labelColName,
                                 String selectorType,
                                 int numTopFeatures,
                                 double percentile,
                                 double fpr,
                                 double fdr,
                                 double fwe) {
        DataSet<Row> chiSquareTest =
            ChiSquareTestUtil.test(in, selectedColNames, labelColName);

        DataSet<Row> model = chiSquareTest.mapPartition(
            new ChiSquareTest.ChiSquareSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe))
            .name("FilterFeature")
            .setParallelism(1);

        return DataSetConversionUtil.toTable(in.getMLEnvironmentId(), model, new ChiSqSelectorModelDataConverter().getModelSchema());
    }

    /**
     * chi-square selector for vector data.
     * @param in: input
     * @param selectedColName: selected vector name
     * @param labelColName: label col name
     * @param selectorType: "numTopFeatures", "percentile", "fpr", "fdr", "fwe"
     * @param numTopFeatures: if selectorType is numTopFeatures, select the largest numTopFeatures features.
     * @param percentile: if selectorType is percentile, select the largest percentile * numFeatures features.
     * @param fpr: if selectorType is fpr, select feature which chi-square value less than fpr.
     * @param fdr: if selectorType is fdr, select feature which chi-square value less than fdr * (i + 1) / n.
     * @param fwe: if selectorType is fwe, select feature which chi-square value less than fwe / n.
     * @return selected col indices.
     */
    public static Table vectorSelector(BatchOperator in,
                                       String selectedColName,
                                       String labelColName,
                                       String selectorType,
                                       int numTopFeatures,
                                       double percentile,
                                       double fpr,
                                       double fdr,
                                       double fwe) {
        DataSet<Row> chiSquareTest =
            ChiSquareTestUtil.vectorTest(in, selectedColName, labelColName);

        DataSet<Row> model = chiSquareTest.mapPartition(
            new ChiSquareTest.ChiSquareSelector(selectorType, numTopFeatures, percentile, fpr, fdr, fwe))
            .name("FilterFeature")
            .setParallelism(1);

        return DataSetConversionUtil.toTable(in.getMLEnvironmentId(), model, new ChiSqSelectorModelDataConverter().getModelSchema());
    }


    /**
     * build chi-square test result, it is for table and vector.
     */
    public static Table buildResult(DataSet<Row> in,
                                    String[] selectedColNames,
                                    Long sessionId) {

        String[] outColNames = new String[]{"col", "chisquare_test"};
        TypeInformation[] outColTypes;
        if (selectedColNames == null) {
            outColTypes = new TypeInformation[]{Types.LONG, Types.STRING};
        } else {
            outColTypes = new TypeInformation[]{Types.STRING, Types.STRING};
        }
        return DataSetConversionUtil.toTable(sessionId,
            in.map(new BuildResult(selectedColNames)),
            outColNames,
            outColTypes);
    }

    /**
     * chi-square test build result.
     */
    private static class BuildResult implements MapFunction<Row, Row> {
        private String[] selectedColNames;

        BuildResult(String[] selectedColNames) {
            this.selectedColNames = selectedColNames;
        }

        @Override
        public Row map(Row row) throws Exception {
            double p = (double) row.getField(1);
            double value = (double) row.getField(2);
            double df = (double) row.getField(3);

            ChiSquareTestResult ctr = new ChiSquareTestResult(df, p, value, "chi-square test");

            Row out = new Row(2);
            int id = (Integer) row.getField(0);
            if (selectedColNames != null) {
                out.setField(0, selectedColNames[id]);
            } else {
                out.setField(0, (long) id);
            }
            out.setField(1, JsonConverter.toJson(ctr));

            return out;
        }
    }
}
