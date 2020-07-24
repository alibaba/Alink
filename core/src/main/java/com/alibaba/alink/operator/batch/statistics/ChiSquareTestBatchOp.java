package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.params.statistics.ChiSquareTestParams;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

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
            null,
            getMLEnvironmentId()));

        return this;
    }


    /**
     * Collect result.
     */
    public ChiSquareTestResult[] collectChiSquareTest() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
        return toResult(this.collect());
    }

    /**
     * lazy collect result.
     */
    public final ChiSquareTestBatchOp lazyCollectChiSquareTest(List<Consumer<ChiSquareTestResult[]>> callbacks) {
        this.lazyCollect(d -> {
            ChiSquareTestResult[] summary = toResult(d);
            for (Consumer<ChiSquareTestResult[]> callback : callbacks) {
                callback.accept(summary);
            }
        });
        return this;
    }

    @SafeVarargs
    public final ChiSquareTestBatchOp lazyCollectChiSquareTest(Consumer<ChiSquareTestResult[]>... callbacks) {
        return lazyCollectChiSquareTest(Arrays.asList(callbacks));
    }

    /**
     * lazy print.
     */
    public final ChiSquareTestBatchOp lazyPrintChiSquareTest() {
        return lazyPrintChiSquareTest(null);
    }

    /**
     * lazy print with title.
     */
    public final ChiSquareTestBatchOp lazyPrintChiSquareTest(String title) {
        lazyCollectChiSquareTest(new Consumer<ChiSquareTestResult[]>() {
            @Override
            public void accept(ChiSquareTestResult[] summary) {
                if (title != null) {
                    System.out.println(title);
                }

                System.out.println(PrettyDisplayUtils.displayHeadline("ChiSquareTest", '-'));
                Object[][] data = new Object[summary.length][3];
                for (int i = 0; i < summary.length; i++) {
                    data[i][0] = summary[i].getP();
                    data[i][1] = summary[i].getValue();
                    data[i][2] = summary[i].getDf();
                }
                String re = PrettyDisplayUtils.displayTable(data, summary.length, 3,
                    getSelectedCols(), new String[]{"p", "value", "df"}, "col");
                System.out.println(re);
            }
        });
        return this;
    }


    private ChiSquareTestResult[] toResult(List<Row> rows) {
        //get result
        ChiSquareTestResult[] result = new ChiSquareTestResult[rows.size()];
        String[] selectedColNames = getSelectedCols();

        for (Row row : rows) {
            String colName = (String) row.getField(0);

            TableUtil.assertSelectedColExist(selectedColNames, colName);

            result[TableUtil.findColIndexWithAssertAndHint(selectedColNames, colName)] =
                JsonConverter.fromJson((String) row.getField(1), ChiSquareTestResult.class);
        }

        return result;
    }
}
