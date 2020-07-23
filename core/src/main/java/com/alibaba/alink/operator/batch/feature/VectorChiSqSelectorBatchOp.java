package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.ChiSqSelectorModelDataConverter;
import com.alibaba.alink.operator.common.feature.ChisqSelectorModelInfo;
import com.alibaba.alink.operator.common.feature.ChisqSelectorModelInfoBatchOp;
import com.alibaba.alink.operator.common.feature.ChisqSelectorUtil;
import com.alibaba.alink.operator.common.statistics.ChiSquareTestUtil;
import com.alibaba.alink.params.feature.VectorChiSqSelectorParams;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;


/**
 * chi-square selector for vector.
 */
public final class VectorChiSqSelectorBatchOp extends BatchOperator<VectorChiSqSelectorBatchOp>
    implements VectorChiSqSelectorParams<VectorChiSqSelectorBatchOp>,
    WithModelInfoBatchOp<ChisqSelectorModelInfo, VectorChiSqSelectorBatchOp, ChisqSelectorModelInfoBatchOp> {

	private static final long serialVersionUID = 2668694739982519452L;

	public VectorChiSqSelectorBatchOp() {
        super(null);
    }

    public VectorChiSqSelectorBatchOp(Params params) {
        super(params);
    }

    @Override
    public VectorChiSqSelectorBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String vectorColName = getSelectedCol();
        String labelColName = getLabelCol();

        SelectorType selectorType = getParams().get(SELECTOR_TYPE);
        int numTopFeatures = getParams().get(NUM_TOP_FEATURES);
        double percentile = getParams().get(PERCENTILE);
        double fpr = getParams().get(FPR);
        double fdr = getParams().get(FDR);
        double fwe = getParams().get(FWE);

        DataSet<Row> chiSquareTest =
            ChiSquareTestUtil.vectorTest(in, vectorColName, labelColName);

        DataSet<Row> model = chiSquareTest.mapPartition(
            new ChisqSelectorUtil.ChiSquareSelector(null, selectorType, numTopFeatures, percentile, fpr, fdr, fwe))
            .name("FilterFeature")
            .setParallelism(1);

        setOutputTable(DataSetConversionUtil.toTable(in.getMLEnvironmentId(), model, new ChiSqSelectorModelDataConverter().getModelSchema()));

        return this;
    }

    @Override
    public ChisqSelectorModelInfoBatchOp getModelInfoBatchOp() {
        return new ChisqSelectorModelInfoBatchOp(getParams()).linkFrom(this);
    }
}
