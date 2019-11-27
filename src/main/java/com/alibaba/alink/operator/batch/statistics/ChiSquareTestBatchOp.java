package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.params.statistics.ChiSquareTestParams;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * Chi-square test is chi-square independence test.
 * Chi-square independence test is to test whether two factors affect each other.
 * Its zero hypothesis is that the two factors are independent of each other.
 * More information on chi-square test: http://en.wikipedia.org/wiki/Chi-squared_test
 */
public final class ChiSquareTestBatchOp extends BatchOperator<ChiSquareTestBatchOp>
    implements ChiSquareTestParams<ChiSquareTestBatchOp> {

    /**
     * default constructor
     */
    public ChiSquareTestBatchOp() {
        super(null);
    }

    public ChiSquareTestBatchOp(Params params) {
        super(params);
    }

    /**
     * overwrite linkFrom in BatchOperator
     *
     * @param inputs input batch op
     * @return BatchOperator
     */
    @Override
    public ChiSquareTestBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String[] selectedColNames = getSelectedCols();
        String labelColName = getLabelCol();

        Preconditions.checkArgument(!ArrayUtils.isEmpty(selectedColNames),
            "selectedColNames must be set.");

        TableUtil.assertSelectedColExist(in.getColNames(), selectedColNames);
        TableUtil.assertSelectedColExist(in.getColNames(), labelColName);

        this.setOutputTable(ChiSquareTestUtil.buildResult(
            ChiSquareTestUtil.test(in, selectedColNames, labelColName),
            selectedColNames,
            getMLEnvironmentId()));

        return this;
    }


    public ChiSquareTestResult[] collectChiSquareTestResult() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");

        List<Row> rows = this.collect();

        //get result
        ChiSquareTestResult[] result = new ChiSquareTestResult[rows.size()];
        String[] selectedColNames = getSelectedCols();

        for (Row row : rows) {
            String colName = (String) row.getField(0);

            TableUtil.assertSelectedColExist(selectedColNames, colName);

            result[TableUtil.findColIndex(selectedColNames, colName)] =
                JsonConverter.fromJson((String) row.getField(1), ChiSquareTestResult.class);
        }

        return result;
    }

}
