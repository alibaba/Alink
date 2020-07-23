package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestResult;
import com.alibaba.alink.operator.common.utils.PrettyDisplayUtils;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.params.statistics.VectorChiSquareTestParams;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.function.Consumer;

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
            selectedColName,
            getMLEnvironmentId()));

        return this;
    }

    /**
     * @return ChiSquareTestResult[]
     */
    public ChiSquareTestResult[] collectChiSquareTest() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
        return toResult(this.collect());
    }

    @SafeVarargs
    public final VectorChiSquareTestBatchOp lazyCollectChiSquareTest(Consumer<ChiSquareTestResult[]>... callbacks) {
        this.lazyCollect(d -> {
            ChiSquareTestResult[] summary = toResult(d);
            for (Consumer<ChiSquareTestResult[]> callback : callbacks) {
                callback.accept(summary);
            }
        });
        return this;
    }

    public final VectorChiSquareTestBatchOp lazyPrintChiSquareTest() {
        return lazyPrintChiSquareTest(null);
    }

    public final VectorChiSquareTestBatchOp lazyPrintChiSquareTest(String title) {
        lazyCollectChiSquareTest(new Consumer<ChiSquareTestResult[]>() {
            @Override
            public void accept(ChiSquareTestResult[] summary) {
                if (title != null) {
                    System.out.println(title);
                }

                System.out.println(PrettyDisplayUtils.displayHeadline("ChiSquareTest", '-'));
                Object[][] data = new Object[summary.length][3];
                String[] colNames = new String[summary.length];
                for (int i = 0; i < summary.length; i++) {
                    data[i][0] = summary[i].getP();
                    data[i][1] = summary[i].getValue();
                    data[i][2] = summary[i].getDf();
                    colNames[i] = String.valueOf(i);
                }
                String re = PrettyDisplayUtils.displayTable(data, summary.length, 3,
                    colNames, new String[]{"p", "value", "df"}, "col");
                System.out.println(re);
            }
        });
        return this;
    }

    private ChiSquareTestResult[] toResult(List<Row> rows) {
        ChiSquareTestResult[] result = new ChiSquareTestResult[rows.size()];

        for (Row row : rows) {
            int id = Integer.parseInt(String.valueOf(row.getField(0)));
            result[id] = JsonConverter.fromJson((String) row.getField(1), ChiSquareTestResult.class);
        }

        return result;
    }

}
