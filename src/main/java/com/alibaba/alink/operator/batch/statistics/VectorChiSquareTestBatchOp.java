package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.params.statistics.VectorChiSquareTestParams;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * Chi-square test is chi-square independence test.
 * Chi-square independence test is to test whether two factors affect each other. Its zero hypothesis is that the two
 * factors are independent of each other.
 * More information on Chi-squared test: http://en.wikipedia.org/wiki/Chi-squared_test
 */
public final class VectorChiSquareTestBatchOp extends BatchOperator<VectorChiSquareTestBatchOp>
    implements VectorChiSquareTestParams<VectorChiSquareTestBatchOp> {

    /**
     * default constructor
     */
    public VectorChiSquareTestBatchOp() {
        super(null);
    }

    public VectorChiSquareTestBatchOp(Params params) {
        super(params);
    }

    /**
     * overwrite linkFrom in BatchOperator
     *
     * @param inputs input batch op
     * @return BatchOperator
     */
    @Override
    public VectorChiSquareTestBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String selectedColName = getSelectedCol();
        String labelColName = getLabelCol();

        if (selectedColName == null) {
            throw new IllegalArgumentException("selectedColName must be set.");
        }

        TableUtil.assertSelectedColExist(in.getColNames(), selectedColName);
        TableUtil.assertSelectedColExist(in.getColNames(), labelColName);

        this.setOutputTable(ChiSquareTestUtil.buildResult(
            ChiSquareTestUtil.vectorTest(in, selectedColName, labelColName)
            , null,
            getMLEnvironmentId()));

        return this;
    }

    /**
     * @return ChiSquareTestResult[]
     */
    public ChiSquareTestResult[] collectChiSquareTestResult() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
        List<Row> rows = this.collect();

        ChiSquareTestResult[] result = new ChiSquareTestResult[rows.size()];

        for (Row row : rows) {
            int id = ((Long) row.getField(0)).intValue();
            result[id] = JsonConverter.fromJson((String) row.getField(1), ChiSquareTestResult.class);
        }

        return result;
    }

}
