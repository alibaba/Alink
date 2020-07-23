package com.alibaba.alink.operator.common.statistics;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
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
     *
     * @param in:            input
     * @param vectorColName: vector col name
     * @param labelColName:  label col name
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
     *
     * @param in:               input
     * @param selectedColNames: selected col names
     * @param labelColName:     label col name
     * @return chi-square test result
     */
    public static DataSet<Row> test(BatchOperator in,
                                    String[] selectedColNames,
                                    String labelColName) {
        in = in.select(ArrayUtils.add(selectedColNames, labelColName));
        return ChiSquareTest.test(in.getDataSet(), in.getMLEnvironmentId());
    }

    /**
     * build chi-square test result, it is for table and vector.
     */
    public static Table buildResult(DataSet<Row> in,
                                    String[] selectedCols,
                                    String vectorCol,
                                    Long sessionId) {

        String[] outColNames = new String[]{"col", "chisquare_test"};
        TypeInformation[] outColTypes = new TypeInformation[]{Types.STRING, Types.STRING};
        return DataSetConversionUtil.toTable(sessionId,
            in.map(new BuildResult(selectedCols)),
            outColNames,
            outColTypes);
    }


    /**
     * chi-square test build result.
     */
    private static class BuildResult implements MapFunction<Row, Row> {
        private static final long serialVersionUID = 3043216661405231563L;
        private String[] selectedColNames;

        BuildResult(String[] selectedColNames) {
            this.selectedColNames = selectedColNames;
        }

        @Override
        public Row map(Row row) throws Exception {
            int id = (Integer) row.getField(0);
            double p = (double) row.getField(1);
            double value = (double) row.getField(2);
            double df = (double) row.getField(3);

            String colName = selectedColNames != null ? selectedColNames[id] : String.valueOf(id);

            ChiSquareTestResult ctr = new ChiSquareTestResult(df, p, value, colName);

            Row out = new Row(2);
            out.setField(0, colName);

            out.setField(1, JsonConverter.toJson(ctr));

            return out;
        }
    }

}
